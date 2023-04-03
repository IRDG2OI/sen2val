import os
import netCDF4
import pandas
import geopandas
import xarray
from datetime import date

from osgeo import gdal

from sftp import Sftp
from shape import Shape

from geo_utils_shp_only import get_processed_indices_vect
from geo_utils_shp_only import get_processed_tiles_total_vect
from geo_utils_shp_only import get_processed_tile_vect

# indices_tuiles = {'NVDI' : ['38KQE', '40KCB', ...], 'NDWIGAO' : ['38KQE', '40KCB', ...], ...}
def download_sentinel1_indices(sftp: Sftp, indices_tiles: dict, path_to_dl: str = 'download/ql/',  ext: str = '.tif', limit_download_size:int = 5000000000):
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
                print('An error occurred while browsing', remote_path)
                pass
            i += 1

        if limit_download_size < 0:
            print("----------\n", 'Download size limit reached.', "\n----------\n")
        else:
            print("----------\n", 'Download completed for ' + indice, "\n----------\n")

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
def concat_netcdf(nc_paths: list, concat_file_name: str, path_to_create: str = 'download/nc/'):
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
    3: 'Create a shp for a tile (or region)',
    4: 'Create a csv metadata file with link to data',
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
    sftp = Sftp(
        hostname='sentinel-prod-seas-oi',
        username='g2oi',
        password='achanger'
    )


    # 38KQE Madagascar - 40KCB Réunion
    #indices_tiles = {'NDVI': ['40KCB'], 'NDWIGAO': ['40KCB'], 'MNDWI': ['40KCB']}
    #download_sentinel1_indices(sftp, indices_tiles)

    ''' Open tif files '''
    # tif_path1 = 'download/ql/NDVI/40KCB/S2A_MSIL2A_20160417T063512_N0201_R134_T40KCB_20160417T063510/S2A_MSIL2A_20160417T063512_N0201_R134_T40KCB_20160417T063510_NDVI_CM001_QL.tif'
    # tif_path2 = 'download/ql/NDVI/40KCB/S2A_MSIL2A_20161123T063512_N0204_R134_T40KCB_20161123T063507/S2A_MSIL2A_20161123T063512_N0204_R134_T40KCB_20161123T063507_NDVI_CM001_QL.tif'
    # ds_tif1 = gdal.Open(tif_path1)
    # ds_tif2 = gdal.Open(tif_path2)
    # print('---TIFF---\n', ds_tif1, '\n', ds_tif2)
    #
    ''' Properly close the datasets to flush to disk'''
    # ds_tif1.close()
    # ds_tif2.close() # = None

    # gdal.GetJPEG2000Structure()
    # gdal.FindFile()

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
pour la valorisation et l'accès aux données issue
            de sen2chain
        corentin.souton@ird.fr
    """
    print(logo_g2oi)
    print(logo_sen2val)

    while (True):

        print_menu()
        option = ''
        try:
            option = int(input('Choose an operation: '))
        except:
            print('Wrong input. Please enter a number ...')
        # Check what choice was entered and act accordingly
        if option == 1:
            shp_total()
        elif option == 2:
            shp_indices()
        elif option == 3:
            shp_tuile()
        elif option == 4:
            csv_tuile_indice()
        elif option == 0:
            print('Exiting')
            exit()
        else:
            print('Invalid option. Please enter a number between 0 and 4.')