from pathlib import Path

from osgeo import ogr

"""
Module for collecting configuration data from 
``~/sen2chain_data/config/config.cfg``
"""
from sen2chain.config import SHARED_DATA
from sen2chain.library import Library
from sen2chain.tiles import Tile

TILES_INDEX = SHARED_DATA.get('tiles_index')
TILES_INDEX_DICT = SHARED_DATA.get('tiles_index_dict')

""" Module for manipulating the Sentinel-2 tiles index geopackage file. """

""" 
------------------------------
Returns a vector file of the tile processed indices

    :param out_folder: path to a output file vector.
------------------------------
"""
def get_processed_tiles_vect(out_folder: str = None):

    if not out_folder:
        out_folder = 'download/shp'

    out_shapefile_complement = str(
        Path(out_folder) / 'complement_niv1_indices.shp'
    )
    out_shapefile_total = str(Path(out_folder) / 'niv1_indices_total.shp')


""" 
------------------------------
Returns a vector file of the indice processed tiles

    :param out_folder: path to a output file vector.
------------------------------
"""
def get_processed_indices_vect(out_folder: str = None, file_name:str = None):
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
                'l2a',
                'ndvi',
                'ndwigao',
            ]
        )
        for t in c
    }
    total_index = {
        t
        for c in (
            getattr(lib, toto)
            for toto in [
                'l1c',
                'l2a',
                'ndvi',
                'ndwigao'
            ]
        )
        for t in c
    }
    complement_index = total_index - indices_index

    tile_count = {}
    for key in total_index:
        # ~ logger.info(key)
        tile_count[key] = {
            'l1c': 0,
            'l2a': 0,
            'ndvi': 0,
            'ndwigao': 0,
        }
        tile = Tile(key)
        for p in ['l1c', 'l2a']:
            tile_count[key][p] = len(getattr(tile, p))
        for p in ['ndvi', 'ndwigao']:
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
                   in ['ndvi', 'ndwigao']
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
        'NDWIGAO_L'
    ]:
        field_name = ogr.FieldDefn(name, ogr.OFTString)
        field_name.SetWidth(10)
        out_layer.CreateField(field_name)

        def fill_fields(layer):
            for feat in layer:
                tile_name = feat.GetField('Name')
                tile = Tile(tile_name)

                feat.SetField('L1C', tile_count[tile_name]['l1c'])
                try:
                    feat.SetField(
                        'L1C_F', tile.l1c.first.date.strftime('%d/%m/%Y')
                    )
                    feat.SetField('L1C_L', tile.l1c.last.date.strftime('%d/%m/%Y'))
                except:
                    feat.SetField('L1C_F', 0)
                    feat.SetField('L1C_L', 0)

                feat.SetField('L2A', tile_count[tile_name]['l2a'])
                try:
                    feat.SetField(
                        'L2A_F', tile.l2a.first.date.strftime('%d/%m/%Y')
                    )
                    feat.SetField('L2A_L', tile.l2a.last.date.strftime('%d/%m/%Y'))
                except:
                    feat.SetField('L2A_F', 0)
                    feat.SetField('L2A_L', 0)

                feat.SetField('NDVI', tile_count[tile_name]['ndvi'])
                try:
                    feat.SetField(
                        'NDVI_F',
                        tile.ndvi.masks.cm001.first.date.strftime('%d/%m/%Y'),
                    )
                    feat.SetField(
                        'NDVI_L',
                        tile.ndvi.masks.cm001.last.date.strftime('%d/%m/%Y'),
                    )
                except:
                    feat.SetField('NDVI_F', 0)
                    feat.SetField('NDVI_L', 0)

                feat.SetField('NDWIGAO', tile_count[tile_name]['ndwigao'])
                try:
                    feat.SetField(
                        'NDWIGAO_F',
                        tile.ndwigao.masks.cm001.first.date.strftime('%d/%m/%Y'),
                    )
                    feat.SetField(
                        'NDWIGAO_L',
                        tile.ndwigao.masks.cm001.last.date.strftime('%d/%m/%Y'),
                    )
                except:
                    feat.SetField('NDWIGAO_F', 0)
                    feat.SetField('NDWIGAO_L', 0)

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
