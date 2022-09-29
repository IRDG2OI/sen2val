import os
import netCDF4
import pandas
import geopandas
import xarray

from osgeo import gdal

from sftp import Sftp
from shape import Shape

from geo_utils_shp_only import get_processed_indices_vect
from geo_utils_shp_only import get_processed_tiles_vect

# indices_tuiles = {'NVDI' : ['38KQE', '40KCB', ...], 'NDWIGAO' : ['38KQE', '40KCB', ...], ...}
def download_sentinel1_indices(sftp: Sftp, indices_tiles: dict, path_to_dl: str = 'download/ql/',  ext: str = '.tif', limit_download_size:int = 1000000000):
    '''Connect to SFTP'''
    sftp.connect()
    remote_source_path = '/DATA_SEN2COR/S2_INDICES_SEN2COR/'

    for indice in indices_tiles:
        remote_path = remote_source_path + indice + '/'
        sftp.print_listdir(remote_path)
        indices_tiles_len = len(indices_tiles[indice])
        i = 0

        # --TODO-- si la liste des tuiles pour un indice est vide (ou n'a qu'une seule faleur prédéfini type 'all'),
        #    prendre toutes les tuiles

        while i < indices_tiles_len and limit_download_size >= 0:
            remote_path = remote_source_path + indice + '/' + indices_tiles[indice][i] + '/'
            sftp.print_listdir(remote_path)
            os.makedirs(path_to_dl + indice + '/' + indices_tiles[indice][i] + '/', exist_ok=True)
            limit_download_size = sftp.download_folder(remote_path, path_to_dl + indice + '/' + indices_tiles[indice][i] + '/', limit_download_size, ext)
            i += 1

        if limit_download_size < 0:
            print('Download size limit reached.')

    sftp.disconnect()


def tiff_to_netcdf(tif_path: str, path_to_create: str = 'download/nc/', nc_filename: str = 'gdal_ncdf'):

    os.makedirs(path_to_create, exist_ok=True)

    '''creating empty netcdf'''
    ds_nc = netCDF4.Dataset(path_to_create + nc_filename + '.nc',
                             mode='w')  # 'w' will clobber any existing data (unless clobber=False is used, in which case an exception is raised if the file already exists).

    print('\n---NC SHELL---\n', ds_nc)

    # --TODO--
    # # create nc files from tif with gdal
    # subprocess.call('gdal_translate -of netCDF -co 'FORMAT=NC4' ''+tif_path + '' '' + path_to_create+nc_filename+''')
    # gdal.Translate(path_to_create + nc_filename2, ds_tif2, format='NetCDF')
    #
    # print('\n---NC FULL---\n', ds_nc)
    #
    # for dimension in ds_nc1.dimensions.values():
    #     print(dimension)
    #

    # Properly close the datasets to flush to disk
    ds_nc.close()


# nc_paths = [nc_path1, nc_path2, ...]
def concat_2netcdf(nc_paths: list, concat_file_name: str, path_to_create: str = 'download/nc/'):
    # # GDAL affine transform parameters, According to gdal documentation xoff/yoff are image left corner, a/e are pixel wight/height and b/d is rotation and is zero if image is north up.
    # xoff, a, b, yoff, d, e = ds_tif.GetGeoTransform()

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


if __name__ == '__main__':
    sftp = Sftp(
        hostname='sentinel-prod-seas-oi.univ.run',
        username='user1',
        password='User.1'
    )

    # # 38KQE Madagascar - 40KCB Réunion
    # indices_tiles = {'NDVI': ['38KQE', '40KCB'], 'NDWIGAO': ['38KQE', '40KCB']}
    # download_sentinel1_indices(sftp, indices_tiles)

    ''' Open tif files '''
    # tif_path1 = 'download/ql/NDVI/40KCB/S2A_MSIL2A_20160417T063512_N0201_R134_T40KCB_20160417T063510/S2A_MSIL2A_20160417T063512_N0201_R134_T40KCB_20160417T063510_NDVI_CM001_QL.tif'
    # tif_path2 = 'download/ql/NDVI/40KCB/S2A_MSIL2A_20161123T063512_N0204_R134_T40KCB_20161123T063507/S2A_MSIL2A_20161123T063512_N0204_R134_T40KCB_20161123T063507_NDVI_CM001_QL.tif'
    # ds_tif1 = gdal.Open(tif_path1)
    # ds_tif2 = gdal.Open(tif_path2)
    # print('---TIFF---\n', ds_tif1, '\n', ds_tif2)
    #
    ''' Properly close the datasets to flush to disk'''
    # ds_tif1 = None
    # ds_tif2 = None

    # gdal.GetJPEG2000Structure()
    # gdal.FindFile()

    '''netcdfs to csv'''
    # dict_csv = {'paths': [path_to_create + nc_filename1, path_to_create + nc_filename2], 'type': ['netcdf', 'netcdf']}
    # df_csv = pandas.DataFrame(data=dict_csv)
    # df_csv.to_csv(path_to_create + '/pandacsv.csv', mode='r')
    # df_csv.close()

    '''Shapefile operations'''

    #get_processed_tiles_vect('')
    get_processed_indices_vect('')

    # shp = Shape()
    # data = shp.read_shp(r'./download/shp/niv1_tuiles_total.shp')
    # print(data)

    # shp.write_shp(r'../data/chn_adm2_bak.shp', [spatialref, geomtype, geomlist, fieldlist, reclist])
    # data = None

    '''geoflow metadata file'''
    # md = {'Identifier': ['1', '2'], 'Title': ['row1', 'row2'], 'Description': ['yoshi', 'mario'], 'Subject' :[], 'Creator' :[], 'Date' :[], 'Type' :[], 'Language' :[], 'SpatialCoverage' :[] ,'TemporalCoverage' :[] , 'Format' :[], 'Relation' :[], 'Rights' :[], 'Provenance' :[], 'Data' :[]}
    # md_df = pandas.DataFrame(data=md)
    # md_df.to_csv('download/geoflow.csv')
    # pandas.read_csv('download/geoflow.csv')
    # md_df = None


