import os
import fnmatch
import netCDF4
import pandas
import re
import xarray
from datetime import date
import subprocess
import numpy

import requests

import rasterio
from osgeo import gdal

import dbf

import zipfile
from typing import Union
from pathlib import Path

from sftp import Sftp

from geo_utils_shp_only import get_processed_indices_vect
from geo_utils_shp_only import get_processed_tiles_total_vect
from geo_utils_shp_only import get_processed_tile_vect
from zenodo_helper import *

import gspread
from oauth2client.service_account import ServiceAccountCredentials


def get_tiles_region(file_path: str):

    f = open(file_path, 'r')
    data = f.read()

    '''splitting text'''
    tile_list = data.split("\n")

    '''remove last empty string element'''
    tile_list.pop()

    f.close()
    return tile_list


EXCLUDED = get_tiles_region('tiles_exclude.txt')

CROZET = get_tiles_region('tiles_crozet.txt')
EPARSES = get_tiles_region('tiles_eparses.txt')
KERGUELEN = get_tiles_region('tiles_kerguelen.txt')
MADAGASCAR = get_tiles_region('tiles_mada.txt')
MAURICE = get_tiles_region('tiles_maurice.txt')
REUNION = get_tiles_region('tiles_reunion.txt')
RODRIGUES = get_tiles_region('tiles_rodrigues.txt')
SEYCHELLES = get_tiles_region('tiles_seychelles.txt')
TROMELIN = get_tiles_region('tiles_tromelin.txt')

'''Shapefile operations'''
def shp_total():
    try:
        get_processed_tiles_total_vect('')
        print('general shp created.')
    except:
        print('error while creating general shp file.')


def shp_indices():
    try:
        get_processed_indices_vect('')
        print('shp by indices created.')
    except:
        print('error while creating shp by indices.')


def shp_tuile(tile_name):
    try:
        get_processed_tile_vect('download/shp', tile_name)
        print('shp by tile created.')
    except:
        print('error while creating shp by tile')


def find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        print('find ', files, ' in ', root)
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result


# indices_tuiles = {'NVDI' : ['38KQE', '40KCB', ...], 'NDWIGAO' : ['38KQE', '40KCB', ...], ...}
def download_sentinel1_indices(sftp: Sftp, indices_tiles: dict, path_to_dl: str = 'download/ql/',  ext: str = '.jp2', limit_download_size:int = 5000000000):
    '''Connect to SFTP'''
    sftp.connect()
    remote_source_path = '/DATA_SEN2COR/S2_INDICES_SEN2COR/'

    for indice in indices_tiles:
        remote_path = remote_source_path + indice + '/'
        sftp.print_listdir(remote_path)
        indices_tiles_len = len(indices_tiles[indice])
        i = 0

        # --TODO-- si la liste des tuiles pour un indice est vide (ou n'a qu'une seule valeur prédéfini type 'all'),
        #    prendre toutes les tuiles

        while i < indices_tiles_len and limit_download_size >= 0:
            remote_path = remote_source_path + indice + '/' + indices_tiles[indice][i] + '/'

            try:
                sftp.print_listdir(remote_path)
                os.makedirs(path_to_dl + indice + '/' + indices_tiles[indice][i] + '/', exist_ok=True)
                limit_download_size = sftp.download_folder(remote_path, path_to_dl + indice + '/' + indices_tiles[indice][i] + '/', limit_download_size, ext)
            except FileNotFoundError as err:
                print('An error occurred while browsing', remote_path, err)
                pass
            i += 1

        if limit_download_size < 0:
            print("----------\n", 'Download size limit reached : ', limit_download_size, "\n----------\n")
        else:
            print("----------\n", 'Download completed for ' + indice, "\n----------\n")

    sftp.disconnect()


def create_netcdf(indice_name: str, tile_name : str, start_year: int, path_to_create: str, nc_file_name: str, y_size: int = 0, x_size: int = 0, times_size: int = 0, crs_code:str = '', wkt:str = ''):
    """Create an empty netCDF with given dimension lengths

        Parameters
        ----------
        indice_name : str
            Band name
        tile_name : str
            tile id
        start_year : int
            Year value that start the time serie
        path_to_create : str
            Path to the netCDF file
        nc_file_name : str
            Name of the netCDF file with extension
        y_size : int, optional
            y dimension length
        x_size : int, optional
            x dimension length
        times_size : int, optional
            time dimension length
        crs_code : str, optional
            Code of the coordonates representation system. Found in the JPEG2000 header
        wkt : str, optional
            Spatial representation metadata. Found in the JPEG2000 header
        """
    os.makedirs(path_to_create, exist_ok=True)

    '''creating empty netcdf'''
    ds_nc = netCDF4.Dataset(path_to_create + nc_file_name,
                            mode='w')  # 'w' will clobber any existing data (unless clobber=False is used, in which case an exception is raised if the file already exists).
    ds_nc.title = 'netCDF4 python package product'

    '''Dimensions'''
    ds_nc.createDimension('y', size=y_size,)
    ds_nc.createDimension('x', size=x_size,)
    if times_size > 0:
        ds_nc.createDimension('time', times_size)

    '''Variables'''
    y = ds_nc.createVariable('y', 'f4', ('y',), zlib=True)
    x = ds_nc.createVariable('x', 'f4', ('x',), zlib=True)
    if times_size > 0:
        time = ds_nc.createVariable('time', 'i4', ('time',), zlib=True, fill_value=0)
    crs = ds_nc.createVariable('transverse_mercator', 'c')
    #lon = ds_nc.createVariable('lon', 'f4', ('y', 'x',), zlib=True)
    #lat = ds_nc.createVariable('lat', 'f4', ('y', 'x',), zlib=True)

    indice = ds_nc.createVariable(indice_name, 'f4', ('time', 'y', 'x',), zlib=True, complevel=4, fill_value=-30000)

    '''Attributes'''
    if times_size > 0:
        time.units = 'day (d)'
        time.axis = 'T'
        time.long_name = 'days since 1987-01-01 00:00:00'
        time.standard_name = 'days_since_1987-01-01'

    y.long_name = 'y coordinate of projection'
    y.standard_name = 'projection_y_coordinate'
    y.units = 'm'
    y.axis = 'Y'

    x.long_name = 'x coordinate of projection'
    x.standard_name = 'projection_x_coordinate'
    x.units = 'm'
    x.axis = 'X'

    crs.grid_mapping_name ='transverse_mercator'
    crs.long_name = crs_code
    crs.spatial_ref = wkt

    #lon.long_name = 'longitude'
    #lon.units = 'degrees_east'

    #lat.long_name = 'latitude'
    #lat.units = 'degrees_north'

    indice.long_name = 'Image indices'
    indice.grid_mapping = "transverse_mercator"
    #band.compress = 'time x y'
    #band.coordinates = 'lon lat'

    setattr(ds_nc, 'Conventions', 'CF-1.10')
    setattr(ds_nc, 'start_year', start_year)  # year in the date of the first file met

    #TODO
    # get metadata from shared online drive file

    '''geoflow Dunblincore metadata'''
    geoflow_indice_md_df = pandas.read_csv('metadata/METADATA_SEN2CHAIN_indices.csv', header=0, usecols=lambda col: col not in ['Data'])
    contain_indice_name = geoflow_indice_md_df['Identifier']=='Sen2Chain_'+indice_name
    indice_row = geoflow_indice_md_df[contain_indice_name]

    for col_name in indice_row.columns.values:
        setattr(ds_nc, col_name, indice_row[col_name].values[0]) #indie_row[col_mane] is a series pandas object with 1 value

    print('\n---NC CREATED---\n', ds_nc)
    print(ds_nc.dimensions.keys())
    for variable in ds_nc.variables.values():
         print(variable)

    '''Properly close the datasets to flush to disk'''
    ds_nc.close()


def concat_jpeg_to_netcdf(indice_name: str, tile_name: str, time_index: int, path_jp2: str, output_path: str, nc_file_name:str, ds_nc, time_size: int = 0, overwrite:bool = False):
    """Concat a JPEG2000 file into an existing netCDF or create a new one
            Parameters
            ----------
            indice_name : str
                indice id
            tile_name : str
                tile id
            time_index : str
                index of the image from the netCDF time serie tab
            path_jp2 : str
                path of the JPEG2000 source file to add
            output_path : str
                name and path of the output netCDF file
            ds_nc :
                netCDF dataset to concat at if it already exists
            time_size : int, optional
                length of the time serie dimension
            overwrite : bool, optional
                Overwrite a time serie for the given date if it's correspond to an already existing time serie
                default = false, skipping time serie with the given date and only add data after
            """
    nc_path = output_path + nc_file_name

    img = rasterio.open(path_jp2, driver='JP2OpenJPEG')
    jp2_band1 = img.read(1)  # bands are indexed from 1

    '''Get crs from img metadata'''
    crs = img.crs
    wkt = crs.wkt
    wkt_ar = numpy.array(wkt)
    '''Get the date from img file name'''
    d_str = re.search("(\d{8})", path_jp2).group()  # search the regular expression date pattern and return the first occurrence
    jp2_date_d = int(d_str[6:])
    jp2_date_m = int(d_str[4:-2])
    jp2_date_y = int(d_str[:4])

    if(not os.path.isfile(nc_path)):
        print('NC not found. Creating ' + nc_path)

        height = jp2_band1.shape[0]
        width = jp2_band1.shape[1]

        create_netcdf(indice_name, tile_name, jp2_date_y, output_path, nc_file_name, height, width, time_size, crs, wkt)

        ds_nc = netCDF4.Dataset(nc_path,
                                mode='a')  #a and r+ mean append (in analogy with serial files); an existing file is opened for reading and writing. Appending s to modes r, w, r+ or a will enable unbuffered shared access

        '''Variables'''
        x = ds_nc.variables['x']
        y = ds_nc.variables['y']

        '''Populate x and y variables with data'''
        cols, rows = numpy.arange(height), numpy.arange(width)
        xs, ys = rasterio.transform.xy(img.transform, rows, cols)

        x[:] = numpy.array(xs)
        y[:] = numpy.array(ys)

    else:
        print('NC found. Writing ', nc_path)

    time = ds_nc.variables['time']
    indice = ds_nc.variables[indice_name]

    # xarray.open_dataset(engine=h5netcdf)
    #ds_nc = netCDF4.Dataset(nc_path, mode='a') #a and r+ mean append (in analogy with serial files); an existing file is opened for reading and writing. Appending s to modes r, w, r+ or a will enable unbuffered shared access
    #img1 = cv2.imread(img_path1)  # IMREAD_UNCHANGED

    '''Populate the band and time variables with data'''
    checkdate = date.datetime.strptime("1987-01-01", "%Y-%m-%d")
    time_value = (date.datetime(jp2_date_y, jp2_date_m, jp2_date_d) - checkdate).days

    if (time_value not in time[:]) or overwrite:
        time[time_index] = time_value
        indice[time_index, :, :] = jp2_band1
    else:
        print('Time serie ', time_value, ' already exit. Skipping')

    # print('\n--- NC COMPLETED ---\n')
    # print(ds_nc.dimensions.keys())
    # for variable in ds_nc.variables.values():
    #      print(variable)

    #print('\n--- NC GDAL FULL ---\n')
    #subprocess.call(['gdalinfo', img_path1])

    return ds_nc


def sen2chain_to_netcdf(src_path:str, indices_tiles: dict, output_dir_path: str, ext: str = '.jp2'):
    """Create Netcdf time serie file based on JPEG2000 data directory
        Parameters
        ----------
        src_path : str
            Data directory path
        indices_tiles : dict
            {NDVI:[40KCB, 40KEC, ...], ...}
        output_dir_path : str
            Path to output nc file
        ext : str
            File extension filter string
        """
    #TODO
    # add un '/' at the end of src_path and output_dir_path if this char is not already there

    #TODO
    # donner un indice ou un tuile en param et boucler sur les jp2 trouvés
    # 'enter a indice id : '
    # 'enter a tile id : '

    ds = None

    #TODO
    # read first date and last date for this indice and tile from the shapefile db_total.shp
    years = ["2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023"]

    for indice in indices_tiles:
        for tile in indices_tiles[indice]:
            for y in years:
                print("----------\n", 'Processing for ' + indice + '/' + tile)
                nc_file_name = indice + '_' + tile + '_' + y + '.nc'

                imgs_paths = find('*'+tile+'_'+y+'*'+indice+ext, src_path + indice + '/' + tile + '/')
                print(imgs_paths)
                nb_total_img = len(imgs_paths)  # count the number of the first file.ext in indice directorties. Needed for the limit of time dimension in the nc
                img_incr = 0  # count nb jp2 pour cette tuile et pour cet indice, necessaire comme indice pour la variable temporelle du nc

                if nb_total_img > 0:
                    while img_incr < nb_total_img:
                        print('(' + img_incr + '/' + nb_total_img + ') ' + imgs_paths[img_incr])
                        ds = concat_jpeg_to_netcdf(indice, tile, img_incr, imgs_paths[img_incr], output_dir_path, nc_file_name, ds, nb_total_img)
                        img_incr += 1
                else:
                    print('No ' + ext + ' found in directories ' + src_path + indice + '/' + tile + '/')

                print('Process completed for ' + indice + '/' + tile +" year "+ y , "\n----------\n")

    '''Properly close the datasets to flush to disk'''
    ds.close()


def concat_nc(nc_paths: list, concat_file_name: str, path_to_create: str = 'download/nc/'):
    '''single xarray Dataset containing data from all files'''
    ds_xr = xarray.open_mfdataset(nc_paths, combine='nested', concat_dim='time')
    print('\n---XR---\n', ds_xr)

    '''Specify the path and filename for the concatenated data'''
    outfile = os.path.join(path_to_create, concat_file_name + '.nc')

    '''Write concatenated data to a netCDF file'''
    ds_xr.to_netcdf(outfile)

    ds_nc_concat = netCDF4.Dataset(outfile)
    print('\n---CONCAT---\n', ds_nc_concat)

    '''Properly close the datasets to flush to disk'''
    ds_xr.close()
    ds_nc_concat.close()


def df_to_csv(df: pandas.DataFrame, tile: str):
    """Convert a datadrame based on Geoflow template to a csv by replacing tagged variables based on the given tile id
    Parameters
    ----------
    df : DataFrame
        standard 15 columns Geolow DataFrame with 11 lignes (one by Indice)
    tile : str
        tile identifier
    """

    df['Data'] = ''
    df['SpatialCoverage'] = ''

    df = df.replace(to_replace='.TUILE.', value=tile, regex=True)
    df = df.replace(to_replace='.DATE\]', value=date.today().strftime('%Y-%m-%d'), regex=True)
    df = df.replace(to_replace='.DATE_D.', value='2015', regex=True)
    df = df.replace(to_replace='.DATE_F.', value='2023', regex=True)

    for i in range(0, df.shape[0]):
        description = df.iloc[i, 2].split(':')[1]
        provenance = df.iloc[i, 13].split(':')[1]

        '''concat Provenance text with Description'''
        df.iloc[i, 2] = 'abstract:'+description+"_\ninfo:" +provenance

        '''Add tags based on the localisation of the tile'''
    if tile in CROZET:
        df = df.replace(to_replace='.PAYS.', value='Crozet Islands', regex=True)
        df = df.replace(to_replace='.CONTINENT.', value='Antarctic', regex=True)
    elif tile in EPARSES:
        df = df.replace(to_replace='.PAYS.', value='Scattered Islands', regex=True)
        df = df.replace(to_replace='.CONTINENT.', value='Antarctic', regex=True)
    elif tile in EPARSES:
        df = df.replace(to_replace='.PAYS.', value='Kerguelen Islands', regex=True)
        df = df.replace(to_replace='.CONTINENT.', value='Antarctic', regex=True)
    elif tile in MADAGASCAR:
        df = df.replace(to_replace='.PAYS.', value='Madagascar', regex=True)
        df = df.replace(to_replace='.CONTINENT.', value='Indian Ocean', regex=True)
    elif tile in MAURICE:
        df = df.replace(to_replace='.PAYS.', value='Mauritius', regex=True)
        df = df.replace(to_replace='.CONTINENT.', value='Indian Ocean', regex=True)
    elif tile in REUNION:
        df = df.replace(to_replace='.PAYS.', value='Réunion', regex=True)
        df = df.replace(to_replace='.CONTINENT.', value='Indian Ocean', regex=True)
    elif tile in RODRIGUES:
        df = df.replace(to_replace='.PAYS.', value='Raudrigues', regex=True)
        df = df.replace(to_replace='.CONTINENT.', value='Indian Ocean', regex=True)
    elif tile in SEYCHELLES:
        df = df.replace(to_replace='.PAYS.', value='Seychelles', regex=True)
        df = df.replace(to_replace='.CONTINENT.', value='Indian Ocean', regex=True)
    elif tile in SEYCHELLES:
        df = df.replace(to_replace='.PAYS.', value='Seychelles', regex=True)
        df = df.replace(to_replace='.CONTINENT.', value='Indian Ocean', regex=True)
    elif tile in TROMELIN:
        df = df.replace(to_replace='.PAYS.', value='Tromelin', regex=True)
        df = df.replace(to_replace='.CONTINENT.', value='Indian Ocean', regex=True)

    df.to_csv('download/METADATA_'+tile+'.csv', index=False)
    print('download/METADATA_'+tile+'.csv created')


def zip_dir(src_dir: Union[Path, str], filename: Union[Path, str], wd: str):
    """Zip the provided time serie directory
    Parameters
    ----------
    src_dir : str
        Data directory path
    filename : str
        Path and name of the zip destination file
    wd : str
        File filter string
    """
    # Convert to Path object
    dir = Path(src_dir)

    print('Creating '+filename+' for '+wd)

    # TODO
    #  Do not create an empty zip while there is no data in the directory
    with zipfile.ZipFile(filename, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for entry in dir.rglob(wd):
            zip_file.write(entry, entry.relative_to(dir))


# def update_shp_atr_tabl():
#     db = dbf.Dbf("your_file.dbf")
#
#    ''' Editing a value, assuming you want to edit the first field of the first record'''
#     rec = db[0]
#     rec["FIRST_FIELD"] = "New value"
#     rec.store()
#     del rec
#     db.close()


def zenodo_upload(metadata_path: str):
    """Push entities from a Geoflow csv to a Zenodo deposit

        Parameters
        ----------
        matadata_path : str
            Path to csv metadata
        """
    datatogeoflow_folder = '/home/csouton/Documents/OSUR/pythonProject'
    os.chdir(datatogeoflow_folder)

    # Optional : could start from here => reload dataframe from csv
    df = {}
    # "/home/csouton/Documents/geoflow/test_corentin/metadata/seasoi/METADATA_SEN2CHAIN_40KCB_py.csv"
    df = pandas.read_csv(metadata_path)

    print("#### Upload zip files to Zenodo")
    base_url = "https://zenodo.org/api/"
    for zipul in range(len(df)):
        zenodo_baseurl = base_url
        input = 'weird'
        # data_zip = df.iloc[zipul]['Data'].split('_\n')[0].split('@')[0].split(':')[1]
        data_zip = [df.iloc[zipul]['Data'].split('_\n')[0].split('@')[1]]
        print('data source = ', data_zip[0])
        print("Initialize deposit")
        r = check_token(zenodo_baseurl, ACCESS_TOKEN)
        zenval = zenvar(r)
        print("prereserved doi:" + zenval[1])
        print("Write DOI to dataframe")
        dfzen = df
        if 'id:' in dfzen.iloc[zipul]['Identifier']:
            pass
        else:
            dfzen.iloc[zipul, dfzen.columns.get_loc('Identifier')] = "id:" + dfzen.iloc[zipul][
                'Identifier'] + "_\ndoi:" + zenval[1]
            dfzen.iloc[zipul, dfzen.columns.get_loc("Provenance")] = dfzen.iloc[zipul][
                                                                         "Provenance"] + "_\nprocess:Raw dataset uploaded to " + \
                                                                     base_url.split('api')[0] + "record/" + str(
                zenval[2])
        print("upload data")
        zen_upload = zenul(zenval[0], ACCESS_TOKEN,
                           input, data_zip)

        print("Enrich upload with metadata")
        zen_metadata = zenmdt(zenodo_baseurl, ACCESS_TOKEN, zenval[2], df, zipul)
        zen_metadata.text


def print_menu(list_menu_options):
    for key in list_menu_options.keys():
        print (key, '--', list_menu_options[key] )


def gsheet_to_df(region: list):
    """Get gsheet Geoflow template and for given tiles, create completed csv
    Parameters
    ----------
    region : list
        List of tile ids
    """
    try:
        gc = gspread.service_account(filename='sodium-replica-389612-2dec563dbdbb.json')

        '''read data and put it into a dataframe'''
        spreadsheet = gc.open_by_url(
            'https://docs.google.com/spreadsheets/d/1MKOzmW6nuI9HB0ry051O2NSnXcbvUbzVsVEX0epBnME/edit?usp=sharing')
        # spreadsheet = gc.open_by_key('google_sheet_id')

        workingseet = spreadsheet.worksheet('feuille 0')
        df = pandas.DataFrame(workingseet.get_all_records())
        print('gsheet fetched.')
    except:
        print('error while fecthing gsheet template')

    try:
        for t in region:
            df_to_csv(df, t)
    except:
        print('error while creating csv matadata')


def get_tiles_from_menu():
    while (True):
        print_menu(region_menu_options)
        option = ''
        region = []

        option = input('Enter a tile identifier or choose region number: ')
        y = len(option)
        yy = option.isnumeric()
        yyy = len(option)

        if (len(option)-1 == 1 and option.isnumeric()) or len(option)-1 == 4:
            if len(option)-1 == 4:
                region = [option]
                break
            elif int(option) == 1:
                region = CROZET
                break
            elif int(option) == 2:
                region = EPARSES
                break
            elif int(option) == 3:
                region = KERGUELEN
                break
            elif int(option) == 4:
                region = MADAGASCAR
                break
            elif int(option) == 5:
                region = MAURICE
                break
            elif int(option) == 6:
                region = REUNION
                break
            elif int(option) == 7:
                region = RODRIGUES
                break
            elif int(option) == 8:
                region = SEYCHELLES
                break
            elif int(option) == 9:
                region = TROMELIN
                break
            elif int(option) == 0:
                break
        else:
            print('Wrong input. Please enter a tile identifier or a number between 0 and ',
                  len(region_menu_options) - 1, '.')

    return region


def get_indice_from_menu():
    option = ''

    while (True):
        option = input('Enter an indice identifier or 0 to cancel: ')
        if option in list_indices:
            return option
        else:
            if option == '0':
                return ''
            else:
                print('Wrong input. Please enter an indice identifier from among ' + str(list_indices)[1:-1])

menu_options = {
    1: 'Create a general shp with the coverage of all tiles',
    2: 'Create a shp from indices',
    3: 'Create a shp from region',
    4: 'Create TILE.csv and .json for geoflow',
    5: 'Create netcdf from jp2',
    6: 'Create archive from Indice-Tile time serie',
    7: 'Export TILE_INDICE to zenodo using python',
    8: 'Export TILE_INDICE to zenodo using geoflow',
    0: 'Exit',
}

region_menu_options = {
    1: 'Crozet : '+ str(CROZET)[1:-1],
    2: 'Eparses :'+ str(EPARSES)[1:-1],
    3: 'Kerguelen : '+str(KERGUELEN)[1:-1],
    4: 'Madagascar',
    5: 'Maurice : '+ str(MAURICE)[1:-1],
    6: 'Réunion :' + str(REUNION)[1:-1],
    7: 'Rodrigues :'+ str(RODRIGUES)[1:-1],
    8: 'Seychelles : '+ str(SEYCHELLES)[1:-1],
    9: 'Tromelin :'+ str(TROMELIN)[1:-1],
    0: 'Cancel'
}

list_indices = {
    'NDVI',
    'NDWIGAO',
    'NDWIMCF',
    'MNDWI',
    'IRECI',
    'NDRE',
    'EVI',
    'BIBG',
    'BIGR',
    'BIRNIR',
    'NBR'
}

if __name__ == '__main__':
    os.environ['HDF5_USE_FILE_LOCKING'] = 'FALSE'  # disable all file locking operations used in HDF5

    file = open('sentinel_login.txt','r')

    for line in file:
        ids = line.split(',')

    sftp = Sftp(
        hostname=ids[0],
        username=ids[1],
        password=ids[2]
    )
    file.close()

    # download_sentinel1_indices(sftp, indices_tiles)

    logo_g2oi = """
        /$$$$$$   /$$$$$$   /$$$$$$  /$$$$$$
       /$$__  $$ /$$__  $$ /$$__  $$|_  $$_/
      | $$  \__/|__/  \ $$| $$  \ $$  | $$  
      | $$ /$$$$  /$$$$$$/| $$  | $$  | $$  
      | $$|_  $$ /$$____/ | $$  | $$  | $$  
      | $$  \ $$| $$      | $$  | $$  | $$  
      |  $$$$$$/| $$$$$$$$|  $$$$$$/ /$$$$$$
       \______/ |________/ \______/ |______/
                            
        Grand Observatoire de l'Océan Indien
    """
    logo_sen2val = """
    ___________________________________
                    ____             _ 
     ___  ___ _ __ |___ \__   ____ _| |
    / __|/ _ \ '_ \  __) \ \ / / _` | |
    \__ \  __/ | | |/ __/ \ V / (_| | |
    |___/\___|_| |_|_____| \_/ \__,_|_|
    ___________________________________
Script to complemet products of sen2chain data for Geoflow
        corentin.souton@ird.fr
    """
    print(logo_g2oi)
    print(logo_sen2val)


    while (True):

        print('\n===============\n')
        print_menu(menu_options)
        option = ''
        try:
            option = int(input('Choose an operation: '))
        except:
            print('Wrong input. Please enter a number ... ')
        if option == 1:
            shp_total()

        elif option == 2:
            shp_indices()

        elif option == 3:
            region = get_tiles_from_menu()
            if len(region) > 0:
                for t in region:
                    shp_tuile(t)

            '''Create TILE.csv and .json for geoflo'''
        elif option == 4:
            content = {}
            true = 'true'
            false = 'false'

            region = get_tiles_from_menu()

            if len(region) > 0:
                for t in region:
                    content = {
                        "profile": {
                            "id": "sens2val_" + t + "_metadata",
                            "project": "SEAS-OI - Sens2Chain",
                            "name": "Séries temporelle générés sur la tuile " + t,
                            "organization": "ESPACE-DEV",
                            "logos": [
                                "https://drive.google.com/uc?id=1RSMBdke2znvwtvhoM5evr-1rUesPLH-j"
                            ],
                            "mode": "entity",
                            "options": {
                                "line_separator": "_\n"
                            }
                        },
                        "metadata": {
                            "entities": {
                                "handler": "csv",
                                "source": "/DATA/S2/PRODUCTS/GEOFLOW/metadata/METADATA_" + t + ".csv"
                            },
                            "contacts": {
                                "handler": "{{METADATA_CONTACTS_HANDLER}}",
                                "source": "{{METADATA_CONTACTS}}"
                            }
                        },
                        "software": [
                            {
                                "id": "seasoi-geonetwork",
                                "type": "output",
                                "software_type": "geonetwork",
                                "parameters": {
                                    "url": "{{GEONETWORK_SEASOI_URL}}",
                                    "user": "{{GEONETWORK_USER}}",
                                    "pwd": "{{GEONETWORK_PASSWORD}}",
                                    "version": "4.2.1",
                                    "logger": "DEBUG"
                                }
                            },
                            {
                                "id": "googledrive",
                                "type": "input",
                                "software_type": "googledrive",
                                "parameters": {
                                    "email": "{{GMAIL_USER}}",
                                    "token": ""
                                },
                                "properties": {}
                            },
                            {
                                "id": "seasoi-geoserver",
                                "type": "output",
                                "software_type": "geoserver",
                                "parameters": {
                                    "url": "{{GEOSERVER_SEASOI_URL}}",
                                    "user": "{{GEOSERVER_USER}}",
                                    "pwd": "{{GEOSERVER_PASSWORD}}",
                                    "logger": "DEBUG"
                                },
                                "properties": {
                                    "workspace": "REGION_REUNION"
                                }
                            },
                            {
                                "id": "zenodo",
                                "type": "output",
                                "software_type": "zenodo",
                                "parameters": {
                                    "url": "https://zenodo.org/api",
                                    "token": "{{ ZENODO_SANDBOX_TOKEN }}",
                                    "logger": "DEBUG"
                                },
                                "properties": {
                                    "clean": {
                                        "run": false
                                    }
                                }
                            }
                        ],
                        "actions": [
                            {
                                "id": "geometa-create-iso-19115",
                                "options": {
                                    "logo": false
                                },
                                "run": true
                            },
                            {
                                "id": "geonapi-publish-iso-19139",
                                "run": true
                            },
                            {
                                "id": "geosapi-publish-ogc-services",
                                "run": false,
                                "options": {
                                    "createWorkspace": true,
                                    "createStore": true,
                                    "overwrite_upload": false
                                }
                            },
                            {
                                "id": "zen4R-deposit-record",
                                "run": true,
                                "options": {
                                    "update_files": true,
                                    "depositWithFiles": true,
                                    "deleteOldFiles": true,
                                    "publish": false
                                }
                            }
                        ]
                    }

                    with open('/DATA/S2/PRODUCTS/GEOFLOW/json/sen2val'+t+'.json', "w", encoding='utf-8') as write_file:
                        json.dump(content, write_file)

                gsheet_to_df(region)

            '''Create netcdf from jp2'''
        elif option == 5:
            src_path = 'download/src/'
            output_dir_path = 'download/nc/'
            indices_tiles = {'NDVI': ['38LRK']}
            sen2chain_to_netcdf(src_path, indices_tiles, output_dir_path)

            '''Create archive from Indice-Tile time serie'''
        elif option == 6:
            temporal_coverage = ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023']

            indice = get_indice_from_menu()

            if indice != '':
                region = get_tiles_from_menu()
                for t in region:
                    src_path = '/home/csouton/sen2chain_data/data/INDICES/' + indice + '/' + t
                    for year in temporal_coverage:
                        zip_dir(src_path, 'download/'+indice + '_' + t + '_' + year + '.zip', '*' + year + '*')

            '''Export TILE_INDICE to zenodo using python'''
        elif option == 7:
            region = get_tiles_from_menu()
            for t in region:
                zenodo_upload('download/METADATA_'+t+'.csv')

        elif option == 0:
            print('Exiting')
            exit()

        else:
            print('Invalid option. Please enter a number between 0 and ', len(menu_options)-1, '.')
