from pathlib import Path

from osgeo import ogr

'''
Module for collecting configuration data from 
``~/sen2chain_data/config/config.cfg``
'''
from sen2chain.config import SHARED_DATA
from sen2chain.library import Library
from sen2chain.tiles import Tile

TILES_INDEX = SHARED_DATA.get('tiles_index')
TILES_INDEX_DICT = SHARED_DATA.get('tiles_index_dict')
DEBUG = True

''' Module for manipulating the Sentinel-2 tiles index geopackage file. '''

''' 
------------------------------
Returns a vector file of the tile processed indices

    :param out_folder: path to a output file vector.
------------------------------
'''
def get_processed_indices_vect(out_folder: str = None, file_name: str = None, list_indices = []):

    if not out_folder:
        out_folder = 'download/shp'

    lib = Library()

    if list_indices == []:
        list_indices = [f.name.lower() for f in lib._indices_path.glob("*")]

    tab_attr = {}
    all_attribut = lib.__dict__.items()

    for indice_name in list_indices:
        if indice_name in lib.__dict__.keys():
            if lib.__dict__.get(indice_name) != []:
                tab_attr[indice_name] = lib.__dict__.get(indice_name)

                # Couche complète
                query_str = 'or '.join(
                    ['"{}" = "{}"'.format('Name', idx) for idx in tab_attr[indice_name]]
                )

                if not file_name:
                    file_name = 'niv1_indice'

                out_shapefile_total = str(Path(out_folder) / file_name) + '_'+ indice_name + '.shp'
                drv_gpkg = ogr.GetDriverByName('GPKG')
                input_layer_ds = drv_gpkg.Open(str(TILES_INDEX), 0)
                input_layer_lyr = input_layer_ds.GetLayer(0)

                #ind = set().union(lib.l1c, lib.l2a, ind)
                input_layer_lyr.SetAttributeFilter(query_str)
                driver = ogr.GetDriverByName('ESRI Shapefile')
                out_ds = driver.CreateDataSource(out_shapefile_total)
                out_layer = out_ds.CopyLayer(input_layer_lyr, 'tuiles')
                for name in [
                    'Indice',
                    'F_record',
                    'L_record',
                    'md_link',
                    'DOI'
                ]:
                    field_name = ogr.FieldDefn(name, ogr.OFTString)
                    field_name.SetWidth(15)
                    out_layer.CreateField(field_name)

                    def fill_fields2(layer):
                        for feat in layer:
                            feat.SetField('Indice', indice_name)

                            #feat.SetField('Tuiles', ",".join(str(x) for x in tab_attr[indice_name]))

                            tile_name = feat.GetField('Name')
                            tile = Tile(tile_name)

                            if indice_name == 'ndvi':
                                 feat.SetField('F_record', tile.ndvi.cm001.first.date.strftime('%d/%m/%Y'))
                                 feat.SetField('L_record', tile.ndvi.cm001.last.date.strftime('%d/%m/%Y'))
                            elif indice_name == 'ndwigao':
                                 feat.SetField('F_record', tile.ndwigao.cm001.first.date.strftime('%d/%m/%Y'))
                                 feat.SetField('L_record', tile.ndwigao.cm001.last.date.strftime('%d/%m/%Y'))
                            elif indice_name == 'mndwi':
                                 feat.SetField('F_record', tile.mndwi.cm001.first.date.strftime('%d/%m/%Y'))
                                 feat.SetField('L_record', tile.mndwi.cm001.last.date.strftime('%d/%m/%Y'))
                            else:
                                feat.SetField('F_record', 0)
                                feat.SetField('L_record', 0)

                            feat.SetField('md_link', 'https://g2oi.ird.fr/geonetwork/srv/eng/catalog.search#/metadata/data_'+tile_name.upper()+'_'+indice_name.upper()+'_SEN2CHAIN')
                            feat.SetField('DOI', '')

                            out_layer.SetFeature(feat)

                fill_fields2(out_layer)

    out_layer = None
    del out_layer, out_ds


''' 
------------------------------
Returns a vector file of the indice processed tiles

    :param out_folder: path to a output file vector.
    :param file_name: name of the output shapefile
------------------------------
'''
def get_processed_tiles_total_vect(out_folder: str = None, file_name:str = None):
    if not out_folder:
        out_folder = 'download/shp/'

    if not file_name:
        file_name = 'niv1_tuiles'

    out_shapefile_complement = str(Path(out_folder) / file_name) + '_complement.shp'
    out_shapefile_total = str(Path(out_folder) / file_name) + '_total.shp'
    drv_gpkg = ogr.GetDriverByName('GPKG')
    input_layer_ds = drv_gpkg.Open(str(TILES_INDEX), 0)
    input_layer_lyr = input_layer_ds.GetLayer(0)

    # ~ ndvi_index = Library().ndvi
    # ~ ndwigao_index = Library().ndwigao
    # ~ ndwimcf_index = Library().ndwimcf
    # ~ mndwi_index = Library().mndwi
    # ~ indices_index = set().union(ndvi_index, ndwigao_index, ndwimcf_index, mndwi_index)

    lib = Library()
    indices_index = {
        t
        for c in (
            getattr(lib, toto)
            for toto in [
                'ndvi',
                'ndwigao',
                'mndwi'
            ]
        )
        for t in c
    }
    total_index = {
        t
        for c in (
            getattr(lib, toto)
            for toto in [
                'ndvi',
                'ndwigao',
                'mndwi'
            ]
        )
        for t in c
    }
    complement_index = total_index - indices_index

    tile_count = {}
    for key in total_index:
        # ~ logger.info(key)
        tile_count[key] = {
            'ndvi': 0,
            'ndwigao': 0,
            'mndwi': 0
        }
        tile = Tile(key)
        for p in ['ndvi', 'ndwigao', 'mndwi']:
            try:
                tile_count[key][p] = len(getattr(tile, p).masks.cm001)
            except:
                pass

    # pour supprimer les tuiles ayant tous les indices à 0
    indices_index_nonull = {
        k: v
        for k, v in tile_count.items()
        if sum(
            v[indice]
            for indice in [
                z
                for z in v.keys()
                if z
                   in ['ndvi', 'ndwigao', 'mndwi']
            ]
        )
    }

    # Couche complète
    query_str = 'or '.join(
        ['"{}" = "{}"'.format('Name', idx) for idx in tile_count.keys()]
    )
    input_layer_lyr.SetAttributeFilter(query_str)
    driver = ogr.GetDriverByName('ESRI Shapefile')
    out_ds = driver.CreateDataSource(out_shapefile_total)
    out_layer = out_ds.CopyLayer(input_layer_lyr, 'tuiles')
    for name in [
        'Pays',
        'Zones',
        'NDVI',
        'MD_NDVI',
        'DOI_NDVI',
        'NDVI_F',
        'NDVI_L',
        'NDWIGAO',
        'MD_NDWIGAO',
        'DOI_NDWIGAO',
        'NDWIGAO_F',
        'NDWIGAO_L',
        'MNDWI',
        'MD_MNDWI',
        'DOI_MNDWI',
        'MNDWI_F',
        'MNDWI_L'
    ]:
        field_name = ogr.FieldDefn(name, ogr.OFTString)
        field_name.SetWidth(10)
        out_layer.CreateField(field_name)

        def fill_fields(layer):
            for feat in layer:
                tile_name = feat.GetField('Name')
                tile = Tile(tile_name)

                feat.SetField(
                    'MD_NDVI', 'https://g2oi.ird.fr/geonetwork/srv/eng/catalog.search#/metadata/S2_NDVI'
                )
                feat.SetField('NDVI', tile_count[tile_name]['ndvi'])
                try:
                    feat.SetField(
                        'NDVI_F',
                        tile.ndvi.cm001.first.date.strftime('%d/%m/%Y'),
                    )
                    feat.SetField(
                        'NDVI_L',
                        tile.ndvi.cm001.last.date.strftime('%d/%m/%Y'),
                    )
                except:
                    feat.SetField('NDVI_F', 0)
                    feat.SetField('NDVI_L', 0)

                feat.SetField(
                    'MD_NDWIGAO', 'https://g2oi.ird.fr/geonetwork/srv/eng/catalog.search#/metadata/S2_NDWIGAO'
                )
                feat.SetField('NDWIGAO', tile_count[tile_name]['ndwigao'])
                try:
                    feat.SetField(
                        'NDWIGAO_F',
                        tile.ndwigao.cm001.first.date.strftime('%d/%m/%Y'),
                    )
                    feat.SetField(
                        'NDWIGAO_L',
                        tile.ndwigao.cm001.last.date.strftime('%d/%m/%Y'),
                    )
                except:
                    feat.SetField('NDWIGAO_F', 0)
                    feat.SetField('NDWIGAO_L', 0)

                feat.SetField(
                    'MD_MNDWI', 'https://g2oi.ird.fr/geonetwork/srv/eng/catalog.search#/metadata/S2_MNDWI'
                )
                try:
                    feat.SetField(
                        'MNDWI_F',
                        tile.mndwi.cm001.first.date.strftime('%d/%m/%Y'),
                    )
                    feat.SetField(
                        'MNDWI_L',
                        tile.mndwi.cm001.last.date.strftime('%d/%m/%Y'),
                    )
                except:
                    feat.SetField('MNDWI_F', 0)
                    feat.SetField('MNDWI_L', 0)

                out_layer.SetFeature(feat)

    fill_fields(out_layer)
    out_layer = None
    del out_layer, out_ds

    # Couche complémentaire
    query_str = 'or '.join(
        [
            '"{}" = "{}"'.format('Name', idx)
            for idx in (tile_count.keys() - indices_index_nonull.keys())
        ]
    )
    input_layer_lyr.SetAttributeFilter(query_str)
    driver = ogr.GetDriverByName('ESRI Shapefile')
    out_ds = driver.CreateDataSource(out_shapefile_complement)
    out_layer = out_ds.CopyLayer(input_layer_lyr, 'tuiles')
    for name in [
        'L1C',
        'L1C_F',
        'L1C_L',
        'L2A',
        'L2A_F',
        'L2A_L',
        'NDVI',
        'NDVI_F',
        'NDVI_L',
        'NDWIGAO',
        'NDWIGAO_F',
        'NDWIGAO_L'
    ]:
        field_name = ogr.FieldDefn(name, ogr.OFTString)
        field_name.SetWidth(10)
        out_layer.CreateField(field_name)
    fill_fields(out_layer)
    out_layer = None
    del input_layer_ds, input_layer_lyr, out_layer, out_ds


''' 
------------------------------
Returns a vector file of data processed tiles

    :param out_folder: path to a output file vector.
    :param file_name: name of the output shapefile
------------------------------
'''
def get_processed_tile_vect(out_folder: str = None, tile_name:str = None):

    if DEBUG:
        print('get_processed_tile_vect')

    if not out_folder:
        out_folder = 'download/shp'

    if not tile_name:
        tile_name = 'niv2_tuile'

    out_shapefile_tile = str(Path(out_folder) / tile_name) + '.shp'
    drv_gpkg = ogr.GetDriverByName('GPKG')
    input_layer_ds = drv_gpkg.Open(str(TILES_INDEX), 0)
    input_layer_lyr = input_layer_ds.GetLayer(0)

    # {
    # 40KCB-NDVI: [id, title, desc, creat,... data (tiff)],
    # 40KCB-NDWIGAO: [id, title, desc, creat,... data (tiff)],
    # 38KQE-NDVI: [...],
    # 38KQE-NDWIGAO: [...],
    # }

    lib = Library()

    if DEBUG:
        print('list indice', lib.indices)

    list_tiles_name = {
        t
        for c in (
            getattr(lib, toto)
            for toto in [
            'l1c',
            'l2a',
            'ndvi',
            'ndwigao',
            'mndwi'
        ]
        )
        for t in c
    }
    list_indices_name = [f.name.lower() for f in lib._indices_path.glob("*")]

    tile_count = {}
    # ~ logger.info(key)
    tile_count[tile_name] = {
        'ndvi': 0,
        'ndwigao': 0,
        'mndwi': 0
    }
    tile = Tile(tile_name)

    if DEBUG:
        print('tile', tile)

    for p in ['ndvi', 'ndwigao', 'mndwi']:
        try:
            tile_count[tile_name][p] = len(getattr(tile, p).masks.cm001)
        except:
            pass

    # Couche complète
    query_str = 'or '.join(
        ['"{}" = "{}"'.format('Name', idx) for idx in ['40KCB']]
    )
    input_layer_lyr.SetAttributeFilter(query_str)
    driver = ogr.GetDriverByName('ESRI Shapefile')
    out_ds = driver.CreateDataSource(out_shapefile_tile)
    out_layer = out_ds.CopyLayer(input_layer_lyr, 'tuile')
    for name in [
        'NDVI',
        'NDVI_F',
        'NDVI_L',
        'NDWIGAO',
        'NDWIGAO_F',
        'NDWIGAO_L',
        'MNDWI',
        'MD_MNDWI',
        'DOI_MNDWI',
        'MNDWI_F',
        'MNDWI_L'
    ]:
        field_name = ogr.FieldDefn(name, ogr.OFTString)
        field_name.SetWidth(10)
        out_layer.CreateField(field_name)

        def fill_fields(layer):
            for feat in layer:
                tile_name = feat.GetField('Name')
                tile = Tile(tile_name)

                feat.SetField(
                    'MD_NDVI', 'https://g2oi.ird.fr/geonetwork/srv/eng/catalog.search#/metadata/data_'+tile_name.upper()+'_'+'NDVI'+'_SEN2CHAIN'
                )
                feat.SetField('NDVI', tile_count[tile_name]['ndvi'])
                try:
                    feat.SetField(
                        'NDVI_F',
                        tile.ndvi.cm001.first.date.strftime('%d/%m/%Y'),
                    )
                    feat.SetField(
                        'NDVI_L',
                        tile.ndvi.cm001.last.date.strftime('%d/%m/%Y'),
                    )
                except:
                    feat.SetField('NDVI_F', 0)
                    feat.SetField('NDVI_L', 0)

                feat.SetField(
                    'MD_NDWIGAO', 'https://g2oi.ird.fr/geonetwork/srv/eng/catalog.search#/metadata/data_'+tile_name.upper()+'_'+'NDWIGAO'+'_SEN2CHAIN'
                )
                feat.SetField('NDWIGAO', tile_count[tile_name]['ndwigao'])
                try:
                    feat.SetField(
                        'NDWIGAO_F',
                        tile.ndwigao.cm001.first.date.strftime('%d/%m/%Y'),
                    )
                    feat.SetField(
                        'NDWIGAO_L',
                        tile.ndwigao.cm001.last.date.strftime('%d/%m/%Y'),
                    )
                except:
                    feat.SetField('NDWIGAO_F', 0)
                    feat.SetField('NDWIGAO_L', 0)

                feat.SetField(
                    'MD_MNDWI', 'https://g2oi.ird.fr/geonetwork/srv/eng/catalog.search#/metadata/data_'+tile_name.upper()+'_'+'MNDWI'+'_SEN2CHAIN'
                )
                feat.SetField('MNDWI', tile_count[tile_name]['mndwi'])
                try:
                    feat.SetField(
                        'MNDWI_F',
                        tile.mndwi.cm001.first.date.strftime('%d/%m/%Y'),
                    )
                    feat.SetField(
                        'MNDWI_L',
                        tile.mndwi.cm001.last.date.strftime('%d/%m/%Y'),
                    )
                except:
                    feat.SetField('MNDWI_F', 0)
                    feat.SetField('MNDWI_L', 0)
                out_layer.SetFeature(feat)

    fill_fields(out_layer)
    out_layer = None
    del input_layer_ds, input_layer_lyr, out_layer, out_ds
    if DEBUG:
        print('exit get_processed_tile_vect')
