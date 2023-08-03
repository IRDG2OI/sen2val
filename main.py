import os
import fnmatch
import netCDF4
import pandas
import re
import xarray
from datetime import date
import subprocess
import numpy
import datetime

import rasterio
from osgeo import gdal

from sftp import Sftp

from geo_utils_shp_only import get_processed_indices_vect
from geo_utils_shp_only import get_processed_tiles_total_vect
from geo_utils_shp_only import get_processed_tile_vect

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

def create_netcdf(indice_name: str, tile_name : str, start_year: int, path_to_create: str, nc_file_name: str, y_size: int = 0, x_size: int = 0, times_size: int = 0):

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
    crs.longitude_of_central_meridian = 57.
    crs.false_easting = 500000.
    crs.false_northing = 10000000.
    crs.latitude_of_projection_origin = 0.
    crs.scale_factor_at_central_meridian = 0.9996
    crs.long_name = 'CRS definition'
    crs.GeoTransform ='300000 10 0 7700020 0 -10'
    crs.longitude_of_prime_meridian = 0.
    crs.semi_major_axis = 6378137.
    crs.inverse_flattening = 298.257223563
    crs.spatial_ref = 'PROJCS["WGS 84 / UTM zone 40S",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",57],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",10000000],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","32740"]]'

    #lon.long_name = 'longitude'
    #lon.units = 'degrees_east'

    #lat.long_name = 'latitude'
    #lat.units = 'degrees_north'

    #TODO
    # passer un tableau ou lire un tableur externe pour récupérer les information de l'indice qui seront les attributs de la variable "indice"
    indice.long_name = 'GDAL Band'
    indice.grid_mapping = "transverse_mercator"
    #band.compress = 'time x y'
    #band.coordinates = 'lon lat'

    #TODO
    # The general description of a file’s contents should be contained in the following attributes: title, history, institution, source, comment and references
    # + geoflow

    setattr(ds_nc, 'Conventions', 'CF-1.10')
    setattr(ds_nc, 'start_year', start_year)  # year in the date of the first file met

    print('\n---NC CREATED---\n', ds_nc)
    print(ds_nc.dimensions.keys())
    for variable in ds_nc.variables.values():
         print(variable)

    '''Properly close the datasets to flush to disk'''
    ds_nc.close()


def concat_jpeg_to_netcdf(indice_name: str, tile_name: str, time_index: int, path_jp2: str, output_path: str, nc_file_name:str, time_size: int = 0):

    ds_nc = None
    nc_path = output_path + nc_file_name

    img = rasterio.open(path_jp2, driver='JP2OpenJPEG')
    jp2_band1 = img.read(1)  # bands are indexed from 1

    '''Get the date from img file name'''
    d_str = re.search("(\d{8})", path_jp2).group()  # search the regular expression date pattern and return the first occurrence
    jp2_date_d = int(d_str[6:])
    jp2_date_m = int(d_str[4:-2])
    jp2_date_y = int(d_str[:4])

    if(not os.path.isfile(nc_path)):
        print('NC not found. Creating ' + nc_path)

        height = jp2_band1.shape[0]
        width = jp2_band1.shape[1]

        create_netcdf(indice_name, tile_name, jp2_date_y, output_path, nc_file_name, height, width, time_size)

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

        ds_nc.close()

    else:
        print('NC found. Writing ', nc_path)

    ds_nc = netCDF4.Dataset(nc_path, mode='a') #a and r+ mean append (in analogy with serial files); an existing file is opened for reading and writing. Appending s to modes r, w, r+ or a will enable unbuffered shared access
    #img1 = cv2.imread(img_path1)  # IMREAD_UNCHANGED

    '''Variables'''
    time = ds_nc.variables['time']
    indice = ds_nc.variables[indice_name]

    '''Populate the band and time variables with data'''
    checkdate = datetime.datetime.strptime("1987-01-01", "%Y-%m-%d")
    time_value = (datetime.datetime(jp2_date_y, jp2_date_m, jp2_date_d) - checkdate).days
    time[time_index] = time_value

    indice[time_index, :, :] = jp2_band1

    # print('\n--- NC COMPLETED ---\n')
    # print(ds_nc.dimensions.keys())
    # for variable in ds_nc.variables.values():
    #      print(variable)

    #print('\n--- NC GDAL FULL ---\n')
    #subprocess.call(['gdalinfo', img_path1])

    '''Properly close the datasets to flush to disk'''
    ds_nc.close()


# indices_tuiles = {'NVDI' : ['38KQE', '40KCB', ...], 'NDWIGAO' : ['38KQE', '40KCB', ...], ...}
def sen2chain_to_netcdf(src_path:str, indices_tiles: dict, output_dir_path: str, ext: str = '.jp2'):
    #TODO
    # rajouter un '/' a la fin de src_path et de output_dir_path si le caractère n'est pas présent

    #TODO
    # donner un indice ou un tuile en param et boucler sur les jp2 trouvés
    # 'please enter a indice code : '
    # 'please enter a tile code : '

    for indice in indices_tiles:
        for tile in indices_tiles[indice]:
            print("----------\n", 'Processing for ' + indice + '/' + tile)
            nc_file_name = indice + '_' + tile + '.nc'

            imgs_paths = find('*'+indice+ext, src_path + indice + '/' + tile + '/')
            print(imgs_paths)
            nb_total_img = len(imgs_paths)  # count the number of the first file.ext in indice directorties. Needed for the limit of time dimension in the nc
            img_incr = 0  # count nb jp2 pour cette tuile et pour cet indice, necessaire comme indice pour la variable temporelle du nc

            if nb_total_img > 0:
                while img_incr < nb_total_img:
                    concat_jpeg_to_netcdf(indice, tile, img_incr, imgs_paths[img_incr], output_dir_path, nc_file_name, nb_total_img)
                    img_incr += 1
            else:
                print('No ' + ext + ' found in directories ' + src_path + indice + '/' + tile + '/')
            print('Process completed for ' + indice + '/' + tile, "\n----------\n")

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


def create_metadata_csv_file(indices_tiles, md_output_file_name: str, md_static_content_file_path: str = 'null'):
    '''geoflow metadata file'''

    md = pandas.DataFrame({})
    md_dynamic_content = pandas.DataFrame({})
    md_static_content = pandas.read_csv(md_static_content_file_path, encoding='iso8859_2')

    for indice_name in indices_tiles:
        for tile_name in indices_tiles[indice_name]:

            md_dynamic_content = pandas.concat([md_dynamic_content, pandas.DataFrame(
                {
                    'Identifier': ['data_' + tile_name + '_' + indice_name + '_S2CHAIN'],
                    'title': 'Localisation des séries d\'indices (' + indice_name + ') générés sur la tuile ' + tile_name + ' par Sen2Chain.',
                    'Description': 'abstract:Dans le cadre des activités en télédétection de l\'UMR Espace-DEV, la station SEAS-OI, station de recherche et d\'expertise en' \
                            ' télédétection localisée à St Pierre (974), enregistre depuis 2015 de façon régulière les images brutes des satellites Sentinel-2 sur ' \
                            'certaines zones du sud-ouest de l\'Océan Indien acquises par l\'ESA dans le cadre du programme Copernicus. L’UMR Espace- DEV hébergé à' \
                            ' SEAS-OI a développé avec le CNES, la chaîne de traitement Sen2chain qui permet le calcul des produits Sentinel-2, L1C et L2A, ainsi que' \
                            ' le calcul de huit indices environnementaux.\nLes séries d’indices environnementaux calculés par Sen2Chain depuis 2015 présentent un ' \
                            'grand intérêt pour le suivi et l’observation du territoire dans la zone du sud-ouest de l’Océan Indien pour des projets scientifiques' \
                            ' ou institutionnels en lien avec l\'écologie de la santé (SENS2MALARIA), le changement climatique et la gestion des risques naturels ' \
                            'associés dans la région du sud ouest de l\'océan Indien (projet RenovRisk et la chaîne de traitement sur l’analyse des changements ' \
                            'Sen2Change).\nLa couche au format shape permet de localiser et d\'accéder aux jeux de données des 3 indices calculés par Sen2chain: ' \
                            'NDVI, NDWIGAO, MNDWI depuis septembre 2015 jusqu\'à maintenant à l\'Ile de la Réunion. Le lien vers ces jeux de données s\'effectue ' \
                            'par l\'interface cartographique et le champ metadata et DOI qui donne accés à la fiche de métadonnées et au DOI associé à chaque jeu ' \
                            'de données.',
                    'Subject': 'theme[General]:G2OI,occupation du sol, télédétection, Indice'+', '+indice_name+"_\n"
                               'theme[Geographic]:La Réunion, Maurice, Madagascar, Comores, Seychelles, Océan Indien',
                    'Creator':[''],
                    'Date': ['publication:' + str(date.today()) + '_\nedition:' + ''], # trouver des dates dans les données tuiles (date du fichier img etc)
                    'Type': [''],  # tuiles : map, indice : map, tuile-indice : dataset
                    'SpatialCoverage': [''],
                    'TemporalCoverage': [''],  # last and first date of tile
                    'Format': [''],
                    'Relation': 'thumbnail:logo d\'Espace Dev@https://www.espace-dev.fr/wp-content/uploads/2020/03/Logo-Espace-Dev-coul.txt-copie.png_\n ' \
                              'thumbnail:logo de SEAS-OI@http://www.seas-oi.org/image/layout_set_logo?img_id=16402&t=1653032230879_\n' \
                              ' http:sen2extract[site web de SEAS pour la consultation des indices ]@http://indices.seas-oi.org/sen2extract/_\n' \
                              ' http:[site copernicus sentinel-2]@https://sentinels.copernicus.eu/web/sentinel/missions/sentinel-2_\n ' \
                              'http:[site tuiles sentinel-2]https://eatlas.org.au/data/uuid/f7468d15-12be-4e3f-a246-b2882a324f59_\n ' \
                              'http:[site téléchargement tuiles sentinel-2]https://sentinel.esa.int/web/sentinel/missions/sentinel-2/data-products',
                    'Rights':[''],
                    'Provenance':'statement:La chaîne de traitement Sen2Chain utilise le KML fournit par l\'ESA. A partir de ce KML, un shape est généré pour les zones ' \
                                'couvertes dans la zone de l\'Océan Indien pour l\'ensemble des indices fourniés par Sen2Chain. Nous sélectionnons depuis le shape de ' \
                                'Sen2Chain à partir d\'une requête SQL, la tuile 40KCB correspondant au territoire de la Réunion de ce shape dans laquelle nous ' \
                                'retrouvons le lien vers la métadonnées pour les 3 indices NDVI, NDWIGAO, MNDWI. Un champ metadata et un champ DOI permettent ' \
                                'd\'accéder aux jeux de données correspondants.',
                    'Data': ['access:default_\n' +
                             'sourceType:other_\n' +
                             'source:_\n' +
                             'uploadType:other_\n' +
                             'uploadSource:']  # links to imgs
                })
            ])

    #md_output = pandas.merge(md_static_content, md_dynamic_content, on='Type')
    md_dynamic_content.to_csv('download/'+md_output_file_name+'.csv')

    # print(md_static_content.keys())
    md_df = None

menu_options = {
    1: 'Create a general shp with the coverage of all tiles',
    2: 'Create a shp from indices',
    3: 'TODO Create a shp for a tile (or region)',
    4: 'Create a csv metadata file with link to data',
    5: 'Create netcdf from jp2',
    0: 'Exit',
}

def print_menu():
    for key in menu_options.keys():
        print (key, '--', menu_options[key] )

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
def shp_tuile():
    try:
        get_processed_tile_vect('download/shp', '40KCB')
        print('shp by tile created.')
    except:
        print('error while creating shp by tile')

def csv_tuile_indice():
    # 38KQE Madagascar - 40KCB Réunion
    indices_tiles = {'NDVI': ['40KCB'], 'NDWIGAO': ['40KCB'], 'MNDWI': ['40KCB']}

    try:
        create_metadata_csv_file(indices_tiles, 'METADATA_SEN2CHAIN', 'download/METADATA_SEN2CHAIN_tuiles-indices_static.csv')
        print('metadata file created.')
    except:
        print('error while creating metadata file')

if __name__ == '__main__':

    file = open('sentinel_login.txt','r')

    for line in file:
        ids = line.split(',')

    sftp = Sftp(
        hostname=ids[0],
        username=ids[1],
        password=ids[2]
    )

    # 38KQE Madagascar - 40KCB Réunion
    #indices_tiles = {'NDVI': ['40KCB'], 'NDWIGAO': ['40KCB'], 'MNDWI': ['40KCB']}
    #download_sentinel1_indices(sftp, indices_tiles)

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
Script mettant en place une chaîne de traitement 
pour la valorisation et l'accès aux données issues
            de sen2chain
        corentin.souton@ird.fr
    """
    print(logo_g2oi)
    print(logo_sen2val)

    while (True):

        print('\n===============\n')
        print_menu()
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
            shp_tuile()
        elif option == 4:
            csv_tuile_indice()
        elif option == 5:
            src_path = 'download/src/'
            output_dir_path = 'download/nc/'
            indices_tiles = {'NDVI': ['40KCB']}
            sen2chain_to_netcdf(src_path, indices_tiles, output_dir_path)
            
        elif option == 0:
            print('Exiting')
            exit()
        else:
            print('Invalid option. Please enter a number between 0 and ', len(menu_options)-1, '.')
