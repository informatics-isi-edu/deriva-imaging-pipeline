#!/usr/bin/python3

"""
Script for updating the model on Facebase.

Usage:

    python3 schema_updates.py <hostname> <catalog_number> <credentials_file>

"""

import argparse
import json
import sys
from deriva.core import ErmrestCatalog, get_credential
from deriva.core.ermrest_model import builtin_types, Table, Key, ForeignKey, Column
from deriva.core.ermrest_model import tag as chaise_tags

def drop_primary_key_if_exist(catalog, schema_name, table_name, unique_columns):
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    table = schema.tables[table_name]
    constraint_name = '{}_{}_key'.format(table_name, '_'.join(unique_columns))
    try:
        pk = table.keys.__getitem__((schema, constraint_name))
    except:
        pk = None
    
    if pk != None:
        pk.drop()

def drop_foreign_key_if_exist(catalog, schema_name, table_name, foreign_key_columns):
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    table = schema.tables[table_name]
    constraint_name = '{}_{}_fkey'.format(table_name, '_'.join(foreign_key_columns))
    try:
        fk = table.foreign_keys.__getitem__((schema, constraint_name))
    except:
        fk = None
    
    if fk != None:
        fk.drop()

def drop_column_if_exist(catalog, schema_name, table_name, column_name):
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    table = schema.tables[table_name]
    if column_name in table.columns.elements:
        column = table.column_definitions.__getitem__(column_name)
        column.drop()

def drop_table_if_exist(catalog, schema_name, table_name):
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name in schema.tables.keys():
        schema.tables[table_name].drop()

"""
Restore the database to the previous status.
"""
def restore(catalog):
    drop_foreign_key_if_exist(catalog, 'isa', 'imaging_data', ['parent_image'])
    drop_foreign_key_if_exist(catalog, 'isa', 'imaging_data', ['processing_status'])
    
    drop_primary_key_if_exist(catalog, 'isa', 'imaging_data', ['RID', 'default_z'])
    
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'image_order')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'notes')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'processing_status')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'pixels_per_meter')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'default_thumbnail_url')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'parent_image')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'default_z')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'series')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'download_tiff_url')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'download_tiff_name')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'download_tiff_bytes')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'download_tiff_md5')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'total_series')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'metadata_url')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'metadata_name')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'metadata_md5')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'metadata_bytes')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'ome_xml_url')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'ome_xml_name')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'ome_xml_md5')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'ome_xml_bytes')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'properties')
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'generated_zs')

    drop_table_if_exist(catalog, 'isa', 'processed_image')
    drop_table_if_exist(catalog, 'isa', 'image_channel')
    drop_table_if_exist(catalog, 'isa', 'image_z')
    drop_table_if_exist(catalog, 'vocab', 'color')
    drop_table_if_exist(catalog, 'vocab', 'display_method')
    drop_table_if_exist(catalog, 'vocab', 'processing_status')

def create_vocabulary_table_if_not_exist(catalog, schema_name, table_name, comment):
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name not in schema.tables:
        schema.create_table(Table.define_vocabulary(table_name, 'FACEBASE:{RID}', key_defs=[Key.define(['Name'], constraint_names=[ [schema_name, '{}_Name_key'.format(table_name)] ])], comment=comment))

def add_rows_to_vocab_processing_status(catalog):

    rows =[
        {'Name': 'new', 'Description': 'A new record was created. It will trigger the processing.'},
        {'Name': 'renew', 'Description': 'The processing will be re-executed.'},
        {'Name': 'success', 'Description': 'The processing was successfully.'},
        {'Name': 'in progress', 'Description': 'The process was started.'},
        {'Name': 'error', 'Description': 'Generic error.'},
        {'Name': 'HATRAC GET ERROR', 'Description': 'An error occurred during a hatrac operation.'},
        {'Name': 'CONVERT ERROR', 'Description': 'An error occurred during the image conversion.'},
        {'Name': 'HTTP ERROR', 'Description': 'An HTTP error occurred.'},
        {'Name': 'GET THUMBNAIL ERROR', 'Description': 'The thumbnail image could not be retrieved from the image server.'},
        {'Name': 'GET TIFF URL ERROR', 'Description': 'The TIFF image could not be retrieved from the image server.'},
        {'Name': 'MISSING_SCENES_WARNING', 'Description': 'The TIFF image has no real scenes.'}
    ]
    pb = catalog.getPathBuilder()
    schema = pb.vocab
    processing_status = schema.processing_status
    processing_status.insert(rows, defaults=['ID', 'URI'])

def add_rows_to_vocab_display_method(catalog):

    rows =[
        {'Name': 'iiif', 'Description': 'International Image Interoperability Framework (iiif)'},
    ]
    pb = catalog.getPathBuilder()
    schema = pb.vocab
    display_method = schema.display_method
    display_method.insert(rows, defaults=['ID', 'URI'])

def add_rows_to_vocab_color(catalog):

    rows =[
        {'Name': 'DAPI', 'Description': 'DAPI'},
        {'Name': 'Combo', 'Description': 'multiple colors'},
        {'Name': 'Red', 'Description': 'Red'},
        {'Name': 'Green', 'Description': 'Green'},
        {'Name': 'Blue', 'Description': 'Blue'},
        {'Name': 'Magenta', 'Description': 'Magenta'},
        {'Name': 'White', 'Description': 'White'}
    ]
    pb = catalog.getPathBuilder()
    schema = pb.vocab
    color = schema.color
    color.insert(rows, defaults=['ID', 'URI'])

def create_processed_image_table(catalog, schema_name):
    table_name = 'processed_image'
    comment = 'Table for storing the metadata of the processed images.'
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name not in schema.tables:
        column_defs = [
            Column.define(
                'reference_image',
                builtin_types.text,
                comment='The RID of the original image.',                        
                nullok=False
                ),
            Column.define(
                'channel_number',
                builtin_types.int4,
                comment='Color channel of this processed image.',
                default=0,                      
                nullok=False
                ),
            Column.define(
                'file_name',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'file_url',
                builtin_types.text,
                comment='The hatrac location.',
                nullok=True
                ),
            Column.define(
                'file_bytes',
                builtin_types.int8,
                nullok=True
                ),
            Column.define(
                'file_md5',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'config',
                builtin_types.jsonb,
                nullok=True
                ),
            Column.define(
                'z_index',
                builtin_types.int4,
                nullok=True
                ),
            Column.define(
                'display_method',
                builtin_types.text,
                nullok=True
                )
            ]

        key_defs = [
            Key.define(['reference_image', 'channel_number', 'z_index'],
                       constraint_names=[['isa', 'processed_image_reference_image_channel_number_z_index_key']]
            )
        ]
        fkey_defs = [
            ForeignKey.define(['display_method'], 'vocab', 'display_method', ['Name'],
                              constraint_names=[['isa', 'processed_image_display_method_fkey']],
                              on_update='CASCADE',
                              on_delete='NO ACTION'   
            ),
            ForeignKey.define(['reference_image', 'channel_number'], 'isa', 'image_channel', ['image', 'channel_number'],
                              constraint_names=[['isa', 'processed_image_reference_image_channel_number_fkey']],
                              on_update='CASCADE',
                              on_delete='NO ACTION'
            ),
            ForeignKey.define(['reference_image'], 'isa', 'imaging_data', ['RID'],
                              constraint_names=[['isa', 'processed_image_reference_image_fkey']],
                              on_update='NO ACTION',
                              on_delete='CASCADE'   
            )
        ]
        table_def = Table.define(
            table_name,
            column_defs,
            key_defs=key_defs,
            fkey_defs=fkey_defs,
            comment=comment,
            provide_system=True
        )
        
        schema.create_table(table_def)
        
def create_image_channel_table(catalog, schema_name):
    table_name = 'image_channel'
    comment = 'Table for storing the channel metadata.'
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name not in schema.tables:
        column_defs = [
            Column.define(
                'image',
                builtin_types.text,
                comment='The image associated with this channel.',                        
                nullok=False
                ),
            Column.define(
                'channel_number',
                builtin_types.int4,
                comment='Which channel (1,2, 3, ...).',
                nullok=False
                ),
            Column.define(
                'legacy_color',
                builtin_types.text,
                comment='Color type (e.g., "DAPI" or "Combo") or color (red, blue, etc.)',
                nullok=True
                ),
            Column.define(
                'name',
                builtin_types.text,
                comment='Channel name (name of the gene/protein displayed on the channel)',
                nullok=False
                ),
            Column.define(
                'image_url',
                builtin_types.text,
                comment='Image URL for this channel (used only in certain image formats).',
                nullok=True
                ),
            Column.define(
                'notes',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'pseudo_color',
                builtin_types.color_rgb_hex,
                nullok=True
                ),
            Column.define(
                'is_rgb',
                builtin_types.boolean,
                nullok=True
                )
            ]

        key_defs = [
            Key.define(['image', 'channel_number'],
                       constraint_names=[['isa', 'image_channel_channel_number_key']]
            )
        ]
        fkey_defs = [
            ForeignKey.define(['image'], 'isa', 'imaging_data', ['RID'],
                              constraint_names=[['isa', 'image_channel_image_fkey']],
                              on_update='CASCADE',
                              on_delete='CASCADE'
            ),
            ForeignKey.define(['legacy_color'], 'vocab', 'color', ['Name'],
                              constraint_names=[['isa', 'image_channel_legacy_color_fkey']],
                              on_update='CASCADE',
                              on_delete='NO ACTION'
            )
        ]
        table_def = Table.define(
            table_name,
            column_defs,
            key_defs=key_defs,
            fkey_defs=fkey_defs,
            comment=comment,
            provide_system=True
        )
        
        schema.create_table(table_def)
        
def create_image_z_table(catalog, schema_name):
    table_name = 'image_z'
    comment = 'Table containing metadata related to generated z of multi-channel images. This table is primarily used for exporting OME Tiff.'
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name not in schema.tables:
        column_defs = [
            Column.define(
                'image',
                builtin_types.text,
                comment='The RID of the original image.',                        
                nullok=False
                ),
            Column.define(
                'z_index',
                builtin_types.text,
                nullok=False
                ),
            Column.define(
                'ome_companion_url',
                builtin_types.text,
                comment='File URL of OME-Tiff companion file associated with a specific z plane.',
                nullok=False
                ),
            Column.define(
                'ome_companion_name',
                builtin_types.text,
                nullok=False
                ),
            Column.define(
                'ome_companion_bytes',
                builtin_types.int8,
                nullok=False
                ),
            Column.define(
                'ome_companion_md5',
                builtin_types.text,
                nullok=False
                )
            ]

        key_defs = [
            Key.define(['image', 'z_index'],
                       constraint_names=[['isa', 'image_z_image_z_index_key']]
            ),
            Key.define(['ome_companion_md5'],
                       constraint_names=[['isa', 'image_z_ome_companion_md5_key']]
            )
        ]
        fkey_defs = [
            ForeignKey.define(['image'], 'isa', 'imaging_data', ['RID'],
                              constraint_names=[['isa', 'image_z_image_fkey']],
                              on_update='CASCADE',
                              on_delete='SET NULL'   
            )
        ]
        table_def = Table.define(
            table_name,
            column_defs,
            key_defs=key_defs,
            fkey_defs=fkey_defs,
            comment=comment,
            provide_system=True
        )
        
        schema.create_table(table_def)

def create_primary_key_if_not_exist(catalog, schema_name, table_name, unique_columns):
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    table = schema.tables[table_name]
    constraint_name = '{}_{}_key'.format(table_name, '_'.join(unique_columns))
    try:
        pk = table.keys.__getitem__((schema, constraint_name))
    except:
        pk = None
    
    if pk == None:
        pkey_def = Key.define(unique_columns, constraint_names=[ [schema_name, constraint_name] ])
        table.create_key(pkey_def)

def create_foreign_key_if_not_exist(catalog, schema_name, table_name, foreign_key_columns, reference_schema, reference_table, referenced_columns, on_update='CASCADE', on_delete='SET NULL'):
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    table = schema.tables[table_name]
    constraint_name = '{}_{}_fkey'.format(table_name, '_'.join(foreign_key_columns))
    try:
        fk = table.foreign_keys.__getitem__((schema, constraint_name))
    except:
        fk = None
    
    if fk == None:
        fkey_def = ForeignKey.define(foreign_key_columns, reference_schema, reference_table, referenced_columns, 
                                     on_update=on_update,
                                     on_delete=on_delete,
                                     constraint_names=[ [schema_name, constraint_name] ])
        table.create_fkey(fkey_def)

def add_column_if_not_exist(catalog, schema_name, table_name, column_name, column_type, default_value, nullok):
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    table = schema.tables[table_name]
    if column_name not in table.columns.elements:
        table.create_column(Column.define(column_name, builtin_types[column_type], default=default_value, nullok=nullok))

def alter_imaging_data_table(catalog):
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'image_order', 'int4', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'notes', 'markdown', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'processing_status', 'text', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'pixels_per_meter', 'int4', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'default_thumbnail_url', 'text', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'parent_image', 'text', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'default_z', 'int4', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'series', 'int4', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'download_tiff_url', 'text', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'download_tiff_name', 'text', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'download_tiff_bytes', 'int8', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'download_tiff_md5', 'text', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'total_series', 'int4', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'metadata_url', 'text', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'metadata_name', 'text', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'metadata_md5', 'text', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'metadata_bytes', 'int8', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'ome_xml_url', 'text', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'ome_xml_name', 'text', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'ome_xml_md5', 'text', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'ome_xml_bytes', 'int8', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'properties', 'jsonb', None, True)
    add_column_if_not_exist(catalog, 'isa', 'imaging_data', 'generated_zs', 'int4', None, True)
    
    create_primary_key_if_not_exist(catalog, 'isa', 'imaging_data', ['RID', 'default_z'])
    
    create_foreign_key_if_not_exist(catalog, 'isa', 'imaging_data', ['processing_status'], 'vocab', 'processing_status', ['Name'], on_update='CASCADE', on_delete='SET NULL')
    create_foreign_key_if_not_exist(catalog, 'isa', 'imaging_data', ['parent_image'], 'isa', 'imaging_data', ['RID'], on_update='CASCADE', on_delete='CASCADE')
    
parser = argparse.ArgumentParser()
parser.add_argument('hostname')
parser.add_argument('catalog_number')
parser.add_argument('credential_file')

args = parser.parse_args()

hostname = args.hostname
catalog_number = args.catalog_number
credential_file = args.credential_file

credentials = get_credential(args.hostname, args.credential_file)
catalog_ermrest = ErmrestCatalog('https', hostname, catalog_number, credentials=credentials)
catalog_ermrest.dcctx['cid'] = 'model'

"""
Restore the database to the previous status.
"""
restore(catalog_ermrest)

"""
Create new vocabulary tables.
"""
create_vocabulary_table_if_not_exist(catalog_ermrest, 'vocab', 'processing_status', 'A set of status for processing an image.')
create_vocabulary_table_if_not_exist(catalog_ermrest, 'vocab', 'display_method', 'Table containing controlled names for image display methods.')
create_vocabulary_table_if_not_exist(catalog_ermrest, 'vocab', 'color', 'Colors and other terms used to describe slide channel appearance.')

"""
Load data into the new vocabulary tables.
"""
add_rows_to_vocab_processing_status(catalog_ermrest)
add_rows_to_vocab_display_method(catalog_ermrest)
add_rows_to_vocab_color(catalog_ermrest)

"""
Create new tables required for the image processing.
"""
create_image_z_table(catalog_ermrest, 'isa')
create_image_channel_table(catalog_ermrest, 'isa')
create_processed_image_table(catalog_ermrest, 'isa')

"""
Add new columns, primary keys and foreign keys to the imaging_data table.
"""
alter_imaging_data_table(catalog_ermrest)
