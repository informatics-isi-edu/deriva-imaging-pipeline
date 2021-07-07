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
from deriva.core.ermrest_model import builtin_types, Table, Schema, Key, ForeignKey, Column
from deriva.core.ermrest_model import tag as chaise_tags
from ast import literal_eval

facebase_users = ["https://auth.globus.org/143f5bdc-c127-11e4-ab32-22000a1dd033"]
facebase_admins = ["https://auth.globus.org/3dafcdea-fbfa-11e4-86df-22000aa51e6e","https://dev.facebase.org/webauthn_robot/fb_cron","https://staging.facebase.org/webauthn_robot/fb_cron","https://www.facebase.org/webauthn_robot/fb_cron"]
facebase_curators = ["https://auth.globus.org/8438a12e-6589-11e7-9091-22000b500e8d","https://dev.facebase.org/webauthn_robot/fb_cron","https://staging.facebase.org/webauthn_robot/fb_cron","https://www.facebase.org/webauthn_robot/fb_cron"]
facebase_writers = ["https://auth.globus.org/01c7bf28-2622-11e7-9ad7-22000b74c0b7"]
facebase_dar_users = ["https://auth.globus.org/c0a2bf2d-9179-11ea-82db-0a875b138121"]

admins = facebase_admins
curators = facebase_curators
writers = facebase_writers
writers_and_curators = curators + writers
users = facebase_users + writers_and_curators
dar_users = facebase_dar_users + facebase_curators

isa_curation_policy = {"insert": writers_and_curators, "update": curators, "delete": curators}
schema_acls = isa_curation_policy

restricted_visibility_policy = {"select": curators}
table_acls = restricted_visibility_policy

source_definitions = {
    "columns": True,
    "fkeys": True,
    "sources": {
        "imaging_rows": {
            "source": [{"inbound": ["Imaging", "Image_Primary_Table_imaging_data_RID_fkey"]}, "RID"],
            "entity": True,
            "aggregate": "array_d"
        }
    }
}

virtual_column = {
   "markdown_name": "Image",
   "display": {
       "wait_for": ["imaging_rows"],
       "template_engine": "handlebars",
       "markdown_pattern": "{{#if (eq processing_status \"success\")}} {{#each imaging_rows}}{{#if this.values.Generated_Zs}}::: iframe [](/chaise/viewer/#1/Imaging:Image/RID={{#encode this.values.RID}}{{/encode}}){width=1000 height=1000} \n:::{{else}}Multi Scenes{{/if}}{{/each}}{{else}}Unavailable{{/if}}"
   }
 }

dataset_suppl_released_guard = """
{
    "types": [
      "select"
    ],
    "projection": [
        %INCLUDE%
        {
          "outbound": [
            "Imaging",
            "Image_Primary_Table_imaging_data_RID_fkey"
          ]
        },
        {
          "outbound": [
            "isa",
            "imaging_data_dataset_fkey"
          ]
        },
        {
          "filter": "released",
          "operator": "=",
          "operand": True
        },
        "RID"
    ],
    "projection_type": "nonnull"
}
"""

dataset_suppl_edit_guard = """
{
    "types": [
      "select",
      "update",
      "delete"
    ],
    "scope_acl": writers,
    "projection": [
        %INCLUDE%
        {
          "outbound": [
            "Imaging",
            "Image_Primary_Table_imaging_data_RID_fkey"
          ]
        },
        {
          "outbound": [
            "isa",
            "imaging_data_dataset_fkey"
          ]
        },
        {
          "outbound": [
            "isa",
            "dataset_project_fkey"
          ]
        },
        {
          "outbound": [
            "isa",
            "project_groups_fkey"
          ]
        },
        "groups"
    ],
    "projection_type": "acl"
}
"""

image_annotation_include = """
        {
          "outbound": [
            "Imaging",
            "Image_Annotation_Image_fkey"
          ]
        },
"""

image_annotation_file_include = """
        {
          "outbound": [
            "Imaging",
            "Image_Annotation_File_Image_fkey"
          ]
        },
"""

processed_image_include = """
        {
          "outbound": [
            "Imaging",
            "Processed_Image_Reference_Image_fkey"
          ]
        },
"""

image_z_include = """
        {
          "outbound": [
            "Imaging",
            "Image_Z_Image_fkey"
          ]
        },
"""

image_channel_include = """
        {
          "outbound": [
            "Imaging",
            "Image_Channel_Image_fkey"
          ]
        },
"""

image_dataset_suppl_released_guard = literal_eval(dataset_suppl_released_guard.replace('%INCLUDE%', '').replace('writers', str(writers)))
image_dataset_suppl_edit_guard = literal_eval(dataset_suppl_edit_guard.replace('%INCLUDE%', '').replace('writers', str(writers)))

image_annotation_dataset_suppl_released_guard = literal_eval(dataset_suppl_released_guard.replace('%INCLUDE%', image_annotation_include).replace('writers', str(writers)))
image_annotation_dataset_suppl_edit_guard = literal_eval(dataset_suppl_edit_guard.replace('%INCLUDE%', image_annotation_include).replace('writers', str(writers)))

image_annotation_file_dataset_suppl_released_guard = literal_eval(dataset_suppl_released_guard.replace('%INCLUDE%', image_annotation_file_include).replace('writers', str(writers)))
image_annotation_file_dataset_suppl_edit_guard = literal_eval(dataset_suppl_edit_guard.replace('%INCLUDE%', image_annotation_file_include).replace('writers', str(writers)))

processed_image_dataset_suppl_released_guard = literal_eval(dataset_suppl_released_guard.replace('%INCLUDE%', processed_image_include).replace('writers', str(writers)))
processed_image_dataset_suppl_edit_guard = literal_eval(dataset_suppl_edit_guard.replace('%INCLUDE%', processed_image_include).replace('writers', str(writers)))

image_z_dataset_suppl_released_guard = literal_eval(dataset_suppl_released_guard.replace('%INCLUDE%', image_z_include).replace('writers', str(writers)))
image_z_dataset_suppl_edit_guard = literal_eval(dataset_suppl_edit_guard.replace('%INCLUDE%', image_z_include).replace('writers', str(writers)))

image_channel_dataset_suppl_released_guard = literal_eval(dataset_suppl_released_guard.replace('%INCLUDE%', image_channel_include).replace('writers', str(writers)))
image_channel_dataset_suppl_edit_guard = literal_eval(dataset_suppl_edit_guard.replace('%INCLUDE%', image_channel_include).replace('writers', str(writers)))

image_acl_bindings = {"dataset_suppl_released_guard": image_dataset_suppl_released_guard,
                      "dataset_suppl_edit_guard": image_dataset_suppl_edit_guard
}

image_annotation_acl_bindings = {"dataset_suppl_released_guard": image_annotation_dataset_suppl_released_guard,
                      "dataset_suppl_edit_guard": image_annotation_dataset_suppl_edit_guard
}

image_annotation_file_acl_bindings = {"dataset_suppl_released_guard": image_annotation_file_dataset_suppl_released_guard,
                      "dataset_suppl_edit_guard": image_annotation_file_dataset_suppl_edit_guard
}

processed_image_acl_bindings = {"dataset_suppl_released_guard": processed_image_dataset_suppl_released_guard,
                      "dataset_suppl_edit_guard": processed_image_dataset_suppl_edit_guard
}

image_z_acl_bindings = {"dataset_suppl_released_guard": image_z_dataset_suppl_released_guard,
                      "dataset_suppl_edit_guard": image_z_dataset_suppl_edit_guard
}

image_channel_acl_bindings = {"dataset_suppl_released_guard": image_channel_dataset_suppl_released_guard,
                      "dataset_suppl_edit_guard": image_channel_dataset_suppl_edit_guard
}

def add_annotation_source_definitions(catalog, schema_name, table_name, value):
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name in schema.tables:
        table = schema.tables[table_name]
        table.annotations['tag:isrd.isi.edu,2019:source-definitions'] = value
        print('Applying source-definitions annotation: {}'.format(value))
        model.apply()
        return
    
def add_annotation_visible_foreign_keys(catalog, schema_name, table_name, value):
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name in schema.tables:
        table = schema.tables[table_name]
        visible_foreign_keys = table.annotations['tag:isrd.isi.edu,2016:visible-foreign-keys']['detailed']
        if value not in visible_foreign_keys:
            visible_foreign_keys.append(value)
            print('Applying visible-foreign-keys annotation: {}'.format(value))
            model.apply()
            return
    
def drop_annotation_foreign_keys(catalog, schema_name, table_name, value):
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name in schema.tables:
        table = schema.tables[table_name]
        visible_foreign_keys = table.annotations['tag:isrd.isi.edu,2016:visible-foreign-keys']['detailed']
        if value in visible_foreign_keys:
            i = 0
            for visible_foreign_key in visible_foreign_keys:
                if visible_foreign_key == value:
                    del visible_foreign_keys[i]
                    model.apply()
                    print('Dropping visible-foreign-keys annotation: {}'.format(value))
                    return
                else:
                    i +=1
    
def add_annotation_visible_columns(catalog, schema_name, table_name, value):
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name in schema.tables:
        table = schema.tables[table_name]
        visible_columns = table.annotations['tag:isrd.isi.edu,2016:visible-columns']
        changed = False
        if value not in visible_columns['detailed']:
            visible_columns['detailed'].append(value)
            changed = True
        if value not in visible_columns['entry']:
            visible_columns['entry'].append(value)
            changed = True
        if changed == True:
            print('Applying visible-columns annotation: {}'.format(value))
            model.apply()
    
def drop_annotation_columns(catalog, schema_name, table_name, value):
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name in schema.tables:
        table = schema.tables[table_name]
        visible_columns = table.annotations['tag:isrd.isi.edu,2016:visible-columns']
        changed = False
        if value in visible_columns['detailed']:
            i = 0
            for visible_column in visible_columns['detailed']:
                if visible_column == value:
                    del visible_columns['detailed'][i]
                    changed = True
                    break
                else:
                    i +=1
        if value in visible_columns['entry']:
            i = 0
            for visible_column in visible_columns['entry']:
                if visible_column == value:
                    del visible_columns['entry'][i]
                    changed = True
                    break
                else:
                    i +=1
        if changed == True:
            print('Dropping visible-columns annotation: {}'.format(value))
            model.apply()
    
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
        print('Dropping Foreign Key: {} of table {}:{}'.format(constraint_name, schema_name, table_name))
        fk.drop()

def drop_column_if_exist(catalog, schema_name, table_name, column_name):
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    table = schema.tables[table_name]
    if column_name in table.columns.elements:
        column = table.column_definitions.__getitem__(column_name)
        print('Dropping column: {} of table {}:{}'.format(column_name, schema_name, table_name))
        column.drop()

def drop_table_if_exist(catalog, schema_name, table_name):
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name in schema.tables.keys():
        print('Dropping table {}:{}'.format(schema_name, table_name))
        schema.tables[table_name].drop()

def drop_schema_if_exist(catalog, schema_name):
    model = catalog.getCatalogModel()
    if schema_name in model.schemas:
        schema = model.schemas[schema_name]
        print('Dropping schema {}'.format(schema_name))
        schema.drop()

"""
Restore the database to the previous status.
"""
def restore(catalog):
    drop_annotation_foreign_keys(catalog, 'isa', 'imaging_data', ['Imaging', 'Image_Primary_Table_imaging_data_RID_fkey'])
    drop_annotation_columns(catalog, 'isa', 'imaging_data', ['isa', 'imaging_data_processing_status_fkey'])
    drop_annotation_columns(catalog, 'isa', 'imaging_data', virtual_column)
    add_annotation_source_definitions(catalog, 'isa', 'imaging_data', None)

    drop_foreign_key_if_exist(catalog, 'isa', 'imaging_data', ['processing_status'])
    
    drop_column_if_exist(catalog, 'isa', 'imaging_data', 'processing_status')
    
    drop_table_if_exist(catalog, 'Imaging', 'Image_Annotation')
    drop_table_if_exist(catalog, 'Imaging', 'Image_Annotation_File')
    drop_table_if_exist(catalog, 'Imaging', 'Processed_Image')
    drop_table_if_exist(catalog, 'Imaging', 'Image_Channel')
    drop_table_if_exist(catalog, 'Imaging', 'Image_Z')
    drop_table_if_exist(catalog, 'Imaging', 'Image')

    drop_table_if_exist(catalog, 'vocab', 'color')
    drop_table_if_exist(catalog, 'vocab', 'display_method')
    drop_table_if_exist(catalog, 'vocab', 'processing_status')
    drop_schema_if_exist(catalog, 'Imaging')

def create_vocabulary_table_if_not_exist(catalog, schema_name, table_name, comment):
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name not in schema.tables:
        schema.create_table(Table.define_vocabulary(table_name, 'FACEBASE:{RID}', key_defs=[Key.define(['Name'], constraint_names=[ [schema_name, '{}_Name_key'.format(table_name)] ])], comment=comment))

def add_rows_to_vocab_processing_status(catalog):

    """
    {'Name': 'HATRAC GET ERROR', 'Description': 'An error occurred during a hatrac operation.'},
    {'Name': 'CONVERT ERROR', 'Description': 'An error occurred during the image conversion.'},
    {'Name': 'HTTP ERROR', 'Description': 'An HTTP error occurred.'},
    {'Name': 'GET THUMBNAIL ERROR', 'Description': 'The thumbnail image could not be retrieved from the image server.'},
    {'Name': 'GET TIFF URL ERROR', 'Description': 'The TIFF image could not be retrieved from the image server.'},
    {'Name': 'MISSING_SCENES_WARNING', 'Description': 'The TIFF image has no real scenes.'}
    """
    rows =[
        {'Name': 'new', 'Description': 'A new record was created. It will trigger the processing.'},
        {'Name': 'renew', 'Description': 'The processing will be re-executed.'},
        {'Name': 'success', 'Description': 'The processing was successfully.'},
        {'Name': 'in progress', 'Description': 'The process was started.'},
        {'Name': 'error', 'Description': 'Generic error.'}
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

def create_processed_image_table_if_not_exists(catalog, schema_name):
    annotations = {
        "tag:isrd.isi.edu,2016:generated": None,
        "tag:isrd.isi.edu,2016:table-display": {
          "compact": {
            "row_markdown_pattern": "[{{{RID}}}:{{{File_Name}}} ({{{File_Bytes}}} bytes)]({{{File_URL}}}){.download-alt}"
          }
        },
        "tag:isrd.isi.edu,2016:visible-columns": {
          "compact": [
            "RID",
            [
              "Imaging",
              "Processed_Image_Reference_Image_fkey"
            ],
            "File_URL",
            "Z_Index",
            "Display_Method",
            "Channel_Number"
          ],
          "detailed": [
            "RID",
            [
              "Imaging",
              "Processed_Image_Reference_Image_fkey"
            ],
            "File_URL",
            "Config",
            "Z_Index",
            "Display_Method",
            "Channel_Number"
          ]
        }
      }

    table_name = 'Processed_Image'
    comment = 'Table for storing the metadata of the processed images.'
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name not in schema.tables:
        column_defs = [
            Column.define(
                'Reference_Image',
                builtin_types.text,
                comment='The RID of the original image.',                        
                nullok=False
                ),
            Column.define(
                'Channel_Number',
                builtin_types.int4,
                comment='Color channel of this processed image.',
                default=0,                      
                nullok=False
                ),
            Column.define(
                'File_Name',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'File_URL',
                builtin_types.text,
                comment='The hatrac location.',
                annotations={
                    "tag:isrd.isi.edu,2017:asset": {
                      "browser_upload": False,
                      "filename_column": "File_Name"
                    }
                  },
                nullok=True
                ),
            Column.define(
                'File_Bytes',
                builtin_types.int8,
                nullok=True
                ),
            Column.define(
                'File_MD5',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'id',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'Config',
                builtin_types.jsonb,
                nullok=True
                ),
            Column.define(
                'Z_Index',
                builtin_types.int4,
                nullok=True
                ),
            Column.define(
                'Display_Method',
                builtin_types.text,
                nullok=True
                )
            ]

        key_defs = [
            Key.define(['Reference_Image', 'Channel_Number', 'Z_Index'],
                       constraint_names=[['Imaging', 'Processed_Image_Reference_Image_Channel_Number_Z_Index_key']]
            )
        ]
        fkey_defs = [
            ForeignKey.define(['Display_Method'], 'vocab', 'display_method', ['Name'],
                              constraint_names=[['Imaging', 'Processed_Image_Display_Method_fkey']],
                              on_update='CASCADE',
                              on_delete='NO ACTION'   
            ),
            ForeignKey.define(['Reference_Image', 'Channel_Number'], 'Imaging', 'Image_Channel', ['Image', 'Channel_Number'],
                              constraint_names=[['Imaging', 'Processed_Image_Reference_Image_Channel_Number_fkey']],
                              on_update='CASCADE',
                              on_delete='NO ACTION'
            ),
            ForeignKey.define(['Reference_Image'], 'Imaging', 'Image', ['RID'],
                              constraint_names=[['Imaging', 'Processed_Image_Reference_Image_fkey']],
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
            annotations=annotations,
            acls=table_acls,
            acl_bindings=processed_image_acl_bindings,
            provide_system=True
        )
        
        schema.create_table(table_def)
        
def create_image_channel_table_if_not_exists(catalog, schema_name):
    annotations = {
        "tag:isrd.isi.edu,2016:table-display": {
          "row_name": {
            "template_engine": "handlebars",
            "row_markdown_pattern": "{{{Channel_Number}}}{{#if Name}}:{{{Name}}}{{/if}}{{#if Notes}} ({{{Notes}}}){{/if}}"
          }
        },
        "tag:isrd.isi.edu,2016:visible-columns": {
          "*": [
            {
              "source": "RID"
            },
            {
              "source": [
                {
                  "outbound": [
                    'Imaging',
                    "Image_Channel_Image_fkey"
                  ]
                },
                "RID"
              ],
              "markdown_name": "Image"
            },
            {
              "source": "Channel_Number"
            },
            {
              "source": "Name"
            },
            {
              "source": "Color_Type"
            },
            {
              "source": "Notes"
            },
            {
              "source": "Config"
            }
          ]
        }
      }

    table_name = 'Image_Channel'
    comment = 'Image Channel'
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name not in schema.tables:
        column_defs = [
            Column.define(
                'Image',
                builtin_types.text,
                comment='The image associated with this channel.',                        
                nullok=False
                ),
            Column.define(
                'Channel_Number',
                builtin_types.int4,
                comment='Which channel (1,2, 3, ...).',
                nullok=False
                ),
            Column.define(
                'Legacy_Color',
                builtin_types.text,
                comment='Color type (e.g., "DAPI" or "Combo") or color (red, blue, etc.)',
                nullok=True
                ),
            Column.define(
                'Name',
                builtin_types.text,
                comment='Channel name (name of the gene/protein displayed on the channel)',
                nullok=False
                ),
            Column.define(
                'Image_URL',
                builtin_types.text,
                comment='Image URL for this channel (used only in certain image formats).',
                nullok=True
                ),
            Column.define(
                'Notes',
                builtin_types.markdown,
                nullok=True
                ),
            Column.define(
                'Pseudo_Color',
                builtin_types.color_rgb_hex,
                nullok=True
                ),
            Column.define(
                'Is_RGB',
                builtin_types.boolean,
                nullok=True
                ),
            Column.define(
                'Config',
                builtin_types.jsonb,
                nullok=True
                )
            ]

        key_defs = [
            Key.define(['Image', 'Channel_Number'],
                       constraint_names=[['Imaging', 'Image_Channel_Imagekey1']]
            )
        ]
        fkey_defs = [
            ForeignKey.define(['Image'], 'Imaging', 'Image', ['RID'],
                              constraint_names=[['Imaging', 'Image_Channel_Image_fkey']],
                              on_update='CASCADE',
                              on_delete='CASCADE'
            ),
            ForeignKey.define(['Legacy_Color'], 'vocab', 'color', ['Name'],
                              constraint_names=[['Imaging', 'Image_Channel_Legacy_Color_fkey']],
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
            annotations=annotations,
            acls=table_acls,
            acl_bindings=image_channel_acl_bindings,
            provide_system=True
        )
        
        schema.create_table(table_def)
        
def create_image_z_table_if_not_exists(catalog, schema_name):
    annotations = {
        "tag:isrd.isi.edu,2016:generated": None,
        "tag:isrd.isi.edu,2016:visible-columns": {
          "*": [
            "RID",
            [
              "Imaging",
              "Image_Z_Image_fkey"
            ],
            "OME_Companion_URL",
            "Z_Index"
          ]
       }
    }
    table_name = 'Image_Z'
    comment = 'Table containing metadata related to generated z of multi-channel images. This table is primarily used for exporting OME Tiff.'
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name not in schema.tables:
        column_defs = [
            Column.define(
                'Image',
                builtin_types.text,
                comment='The RID of the original image.',                        
                nullok=False
                ),
            Column.define(
                'Z_Index',
                builtin_types.text,
                nullok=False
                ),
            Column.define(
                'OME_Companion_URL',
                builtin_types.text,
                comment='File URL of OME-Tiff companion file associated with a specific z plane.',
                annotations={
                    "tag:isrd.isi.edu,2017:asset": {
                      "browser_upload": False,
                      "filename_column": "OME_Companion_Name"
                    }
                  },
                nullok=False
                ),
            Column.define(
                'OME_Companion_Name',
                builtin_types.text,
                nullok=False
                ),
            Column.define(
                'OME_Companion_Bytes',
                builtin_types.int8,
                nullok=False
                ),
            Column.define(
                'OME_Companion_MD5',
                builtin_types.text,
                nullok=False
                )
            ]

        key_defs = [
            Key.define(['Image', 'Z_Index'],
                       constraint_names=[['Imaging', 'Image_Z_Image_Z_Index_key']]
            ),
            Key.define(['OME_Companion_MD5'],
                       constraint_names=[['Imaging', 'Image_Z_OME_Companion_MD5_key']]
            )
        ]
        fkey_defs = [
            ForeignKey.define(['Image'], 'Imaging', 'Image', ['RID'],
                              constraint_names=[['Imaging', 'Image_Z_Image_fkey']],
                              on_update='CASCADE',
                              on_delete='SET NULL'   
            )
        ]
        table_def = Table.define(
            table_name,
            column_defs,
            key_defs=key_defs,
            fkey_defs=fkey_defs,
            annotations=annotations,
            comment=comment,
            acls=table_acls,
            acl_bindings=image_z_acl_bindings,
            provide_system=True
        )
        
        schema.create_table(table_def)

def create_image_annotation_file_table_if_not_exists(catalog, schema_name):
    table_annotations = {
        "tag:isrd.isi.edu,2016:table-display": {
          "compact": {
            "row_order": [
              {
                "column": "RMT",
                "descending": True
              }
            ]
          },
          "row_name": {
            "row_markdown_pattern": "[{{{RID}}}](/chaise/record/#{{{$catalog.snapshot}}}/Imaging:Image_Annotation_File/RID={{{RID}}}): {{{SVG_File_Name}}}"
          }
        },
        "tag:isrd.isi.edu,2016:visible-columns": {
          "*": [
            {
              "source": "RID"
            },
            {
              "source": [
                {
                  "outbound": [
                    "Imaging",
                    "Image_Annotation_File_Image_fkey"
                  ]
                },
                "RID"
              ],
              "markdown_name": "Image"
            },
            {
              "source": "SVG_File"
            },
            {
              "source": "QuPath_Class_File"
            },
            {
              "source": "Notes"
            },
            {
              "source": "Processing_Status"
            },
            {
              "source": "Processing_Detail"
            }
          ],
          "entry": [
            {
              "source": "RID"
            },
            {
              "source": [
                {
                  "outbound": [
                    "Imaging",
                    "Image_Annotation_File_Image_Z_Index_fkey"
                  ]
                },
                "RID"
              ],
              "markdown_name": "Image"
            },
            {
              "source": "SVG_File"
            },
            {
              "source": "QuPath_Class_File"
            },
            {
              "source": "Notes"
            }
          ],
          "detailed": [
            {
              "source": "RID"
            },
            {
              "source": [
                {
                  "outbound": [
                    "Imaging",
                    "Image_Annotation_File_Image_fkey"
                  ]
                },
                "RID"
              ],
              "markdown_name": "Image"
            },
            {
              "source": "Z_Index"
            },
            {
              "source": "SVG_File"
            },
            {
              "source": "QuPath_Class_File"
            },
            {
              "source": "Notes"
            },
            {
              "source": "Processing_Status",
              "display": "{{{Processing_Status}}}{{#if Processing_Detail}}{{{Processing_Detail}}}{{/if}}"
            },
            {
              "source": "Release_Date"
            },
            {
              "display": {
                "template_engine": "handlebars",
                "markdown_pattern": "{{#if (regexMatch $fkeys.Imaging.Image_Annotation_File_Image_fkey.values.Download_Tiff_URL \"\\.tif:\" ) }}{{#unless (regexMatch $fkeys.Imaging.Image_Annotation_File_Image_fkey.values.Download_Tiff_URL \"\\.ome\\.tif:\" ) }} **Preview** of the SVG file over the chosen image (**read only**) \n ::: iframe [{{Image}} (full screen)](/chaise/viewer/#1/Imaging:Image/RID={{{Image}}}?url=/iiif/2/{{#encode 'https://www.gudmap.org'}}{{/encode}}{{#encode $fkeys.Imaging.Image_Annotation_File_Image_fkey.values.Download_Tiff_URL}}{{/encode}}/info.json&url={{{SVG_File}}}){width=1500 height=1500 link=/chaise/viewer/#1/Imaging:Image/id={{{Image}}}?url=/iiif/2/{{#encode 'https://www.gudmap.org'}}{{/encode}}{{#encode $fkeys.Imaging.Image_Annotation_File_Image_fkey.values.Download_Tiff_URL}}{{/encode}}/info.json&url={{{SVG_File}}} resize=both } \n ::: {{/unless}}{{/if}}"
              },
              "markdown_name": "Preview"
            }
          ]
        },
        "tag:isrd.isi.edu,2016:visible-foreign-keys": {
          "*": []
        }
      }
    table_name = 'Image_Annotation_File'
    comment = 'Image annotation file (svg) consisting of multiple groups of overlays associated with different anatomical terms.'
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name not in schema.tables:
        column_defs = [
            Column.define(
                'Image',
                builtin_types.text,
                comment='The RID of the original image.',                        
                nullok=False
                ),
            Column.define(
                'Z_Index',
                builtin_types.int4,
                nullok=True
                ),
            Column.define(
                'Channels',
                builtin_types['int4[]'],
                nullok=True
                ),
            Column.define(
                'SVG_File',
                builtin_types.text,
                comment='SVG overlay file',
                annotations={"tag:isrd.isi.edu,2017:asset": {
                      "md5": "SVG_File_MD5",
                      "url_pattern": "/hatrac/facebase/data/fb3/svg",
                      "filename_column": "SVG_File_Name",
                      "byte_count_column": "SVG_File_Bytes"
                    }
                  },
                nullok=False
                ),
            Column.define(
                'SVG_File_Name',
                builtin_types.text,
                nullok=False
                ),
            Column.define(
                'SVG_File_Bytes',
                builtin_types.int8,
                nullok=False
                ),
            Column.define(
                'SVG_File_MD5',
                builtin_types.text,
                nullok=False
                ),
            Column.define(
                'QuPath_Class_File',
                builtin_types.text,
                comment='QuPath class file ontaining the mapping of colors to class names (i.e. anatomical terms) to be used with the accompanying svg annotation file.',
                annotations={
                    "tag:isrd.isi.edu,2017:asset": {
                      "md5": "QuPath_Class_File_MD5",
                      "url_pattern": "/hatrac/facebase/data/fb3/qupath/{{{QuPath_Class_File_MD5}}}",
                      "filename_column": "QuPath_Class_File_Name",
                      "byte_count_column": "QuPath_Class_File_Bytes"
                    }
                  },
                nullok=True
                ),
            Column.define(
                'QuPath_Class_File_Name',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'QuPath_Class_File_Bytes',
                builtin_types.int8,
                nullok=True
                ),
            Column.define(
                'QuPath_Class_File_MD5',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'Notes',
                builtin_types.markdown,
                nullok=True
                ),
            Column.define(
                'Curation_Status',
                builtin_types.text,
                default='In Preparation',
                nullok=True
                ),
            Column.define(
                'Principal_Investigator',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'Release_Date',
                builtin_types.timestamptz,
                annotations={"tag:isrd.isi.edu,2016:generated": None},
                nullok=True
                ),
            Column.define(
                'Processing_Status',
                builtin_types.text,
                annotations={"tag:isrd.isi.edu,2016:generated": None},
                nullok=True
                ),
            Column.define(
                'Processing_Detail',
                builtin_types.markdown,
                annotations={"tag:isrd.isi.edu,2016:generated": None},
                nullok=True
                )
            ]

        key_defs = [
            Key.define(['SVG_File_MD5'],
                       constraint_names=[['Imaging', 'Image_Annotation_File_SVG_File_MD5_key']]
            )
        ]
        fkey_defs = [
            ForeignKey.define(['Curation_Status'], 'vocab', 'dataset_status', ['id'],
                              constraint_names=[['Imaging', 'Image_Annotation_File_Curation_Status_fkey']],
                              on_update='CASCADE',
                              on_delete='SET NULL'   
            ),
            ForeignKey.define(['Image', 'Z_Index'], 'Imaging', 'Image', ['RID', 'Default_Z'],
                              constraint_names=[['Imaging', 'Image_Annotation_File_Image_Z_Index_fkey']],
                              on_update='CASCADE',
                              on_delete='SET NULL'   
            ),
            ForeignKey.define(['Image'], 'Imaging', 'Image', ['RID'],
                              constraint_names=[['Imaging', 'Image_Annotation_File_Image_fkey']],
                              on_update='CASCADE',
                              on_delete='SET NULL'   
            ),
            ForeignKey.define(['Principal_Investigator'], 'isa', 'person', ['name'],
                              constraint_names=[['Imaging', 'Image_Annotation_File_Principal_Investigator_fkey']],
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
            annotations=table_annotations,
            acls=table_acls,
            acl_bindings=image_annotation_file_acl_bindings,
            provide_system=True
        )
        
        schema.create_table(table_def)

def create_image_annotation_table_if_not_exists(catalog, schema_name):
    table_annotations = {
        "tag:isrd.isi.edu,2016:table-display": {
          "compact": {
            "template_engine": "handlebars",
            "separator_markdown": "\n",
            "row_markdown_pattern": "- {{{RID}}}: [{{{$fkeys.Imaging.Image_Annotation_Anatomy_fkey.values.ID}}}:{{{$fkeys.Imaging.Image_Annotation_Anatomy_fkey.values.Name}}}](/chaise/record/#{{{$catalog.snapshot}}}/Vocabulary:Anatomy/RID={{{$fkeys.Imaging.Image_Annotation_Anatomy_fkey.values.RID}}})"
          }
        },
        "tag:isrd.isi.edu,2016:visible-columns": {
          "*": [
            {
              "source": [
                {
                  "outbound": [
                    "Imaging",
                    "Image_Annotation_Image_fkey"
                  ]
                },
                "RID"
              ]
            },
            {
              "source": [
                {
                  "outbound": [
                    "Imaging",
                    "Image_Annotation_Anatomy_fkey"
                  ]
                },
                "RID"
              ]
            },
            {
              "source": "File_URL"
            },
            {
              "source": "Comments"
            }
          ],
          "entry": [
            {
              "source": [
                {
                  "outbound": [
                    "Imaging",
                    "Image_Annotation_Image_fkey"
                  ]
                },
                "RID"
              ]
            },
            {
              "source": [
                {
                  "outbound": [
                    "Imaging",
                    "Image_Annotation_Anatomy_fkey"
                  ]
                },
                "RID"
              ]
            },
            {
              "source": "File_URL"
            },
            {
              "source": "Comments"
            }
          ],
          "detailed": [
            {
              "source": [
                {
                  "outbound": [
                    "Imaging",
                    "Image_Annotation_Image_fkey"
                  ]
                },
                "RID"
              ]
            },
            {
              "source": [
                {
                  "outbound": [
                    "Imaging",
                    "Image_Annotation_Anatomy_fkey"
                  ]
                },
                "RID"
              ]
            },
            {
              "source": "File_URL"
            },
            {
              "source": "Comments"
            },
            {
              "source": "Z_Index"
            },
            {
              "display": {
                "template_engine": "handlebars",
                "markdown_pattern": "{{#if (regexMatch $fkeys.Imaging.Image_Annotation_Image_fkey.values.Download_Tiff_URL \"\\.tif:\" ) }}{{#unless (regexMatch $fkeys.Imaging.Image_Annotation_Image_fkey.values.Download_Tiff_URL \"\\.ome\\.tif:\" ) }} **Annotation Display** \n ::: iframe [{{Image}} (full screen)](/chaise/viewer/#1/Imaging:Image/id={{{Image}}}?url=/iiif/2/{{#encode 'https://www.gudmap.org'}}{{/encode}}{{#encode $fkeys.Imaging.Image_Annotation_Image_fkey.values.Download_Tiff_URL}}{{/encode}}/info.json&url={{{File_URL}}}){width=1500 height=1500 link=/chaise/viewer/#1/Imaging:Image/id={{{Image}}}?url=/iiif/2/{{#encode 'https://www.gudmap.org'}}{{/encode}}{{#encode $fkeys.Imaging.Image_Annotation_Image_fkey.values.Download_Tiff_URL}}{{/encode}}/info.json&url={{{File_URL}}} resize=both } \n ::: {{/unless}}{{/if}}"
              },
              "markdown_name": "Display"
            }
          ]
        }
      }
    table_name = 'Image_Annotation'
    comment = 'Anatomical annotations associated with an image.'
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name not in schema.tables:
        column_defs = [
            Column.define(
                'Image',
                builtin_types.text,
                comment='The RID of the original image.',                        
                nullok=False
                ),
            Column.define(
                'Anatomy',
                builtin_types.text,
                nullok=False
                ),
            Column.define(
                'File_URL',
                builtin_types.text,
                comment='File URL of associated annotated overlays.',
                annotations={
                    "tag:isrd.isi.edu,2017:asset": {
                      "md5": "File_MD5",
                      "url_pattern": "/hatrac/facebase/data/fb3/annotations/{{$moment.year}}/{{{File_MD5}}}",
                      "filename_column": "File_Name",
                      "byte_count_column": "File_Bytes"
                    }
                  },
                nullok=False
                ),
            Column.define(
                'File_Name',
                builtin_types.text,
                nullok=False
                ),
            Column.define(
                'File_Bytes',
                builtin_types.int8,
                nullok=False
                ),
            Column.define(
                'File_MD5',
                builtin_types.text,
                nullok=False
                ),
            Column.define(
                'Comments',
                builtin_types.markdown,
                nullok=True
                ),
            Column.define(
                'Z_Index',
                builtin_types.int4,
                nullok=True
                ),
            Column.define(
                'Channels',
                builtin_types['int4[]'],
                nullok=True
                ),
            Column.define(
                'Image_Annotation_File',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'Curation_Status',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'Principal_Investigator',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'Release_Date',
                builtin_types.timestamptz,
                annotations={
                    "tag:isrd.isi.edu,2016:generated": None
                  },
                nullok=True
                )
            ]

        key_defs = [
            Key.define(['Image', 'Anatomy', 'Z_Index'],
                       constraint_names=[['Imaging', 'Image_Annotation_Image_Anatomy_Z_Index_key']]
            ),
            Key.define(['Image', 'Anatomy'],
                       constraint_names=[['Imaging', 'Image_Annotation_Image_Anatomy_key']]
            ),
            Key.define(['File_MD5'],
                       constraint_names=[['Imaging', 'Image_Annotation_File_MD5_key']]
            )
        ]
        fkey_defs = [
            ForeignKey.define(['Anatomy'], 'vocab', 'anatomy', ['id'],
                              constraint_names=[['Imaging', 'Image_Annotation_Anatomy_fkey']],
                              on_update='CASCADE',
                              on_delete='SET NULL'   
            ),
            ForeignKey.define(['Image'], 'Imaging', 'Image', ['RID'],
                              constraint_names=[['Imaging', 'Image_Annotation_Image_fkey']],
                              on_update='CASCADE',
                              on_delete='SET NULL'   
            ),
            ForeignKey.define(['Image_Annotation_File'], 'Imaging', 'Image_Annotation_File', ['RID'],
                              constraint_names=[['Imaging', 'Image_Annotation_Image_Annotation_File_fkey']],
                              on_update='CASCADE',
                              on_delete='SET NULL'   
            ),
            ForeignKey.define(['Curation_Status'], 'vocab', 'dataset_status', ['id'],
                              constraint_names=[['Imaging', 'Image_Annotation_Curation_Status_fkey']],
                              on_update='CASCADE',
                              on_delete='SET NULL'   
            ),
            ForeignKey.define(['Principal_Investigator'], 'isa', 'person', ['name'],
                              constraint_names=[['Imaging', 'Image_Annotation_Principal_Investigator_fkey']],
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
            annotations=table_annotations,
            acls=table_acls,
            acl_bindings=image_annotation_acl_bindings,
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

def create_image_table_if_not_exists(catalog, schema_name):
    thumbnail_annotations = {
        "tag:misd.isi.edu,2015:display": {
          "name": "Thumbnail"
        },
        "tag:isrd.isi.edu,2016:column-display": {
          "*": {
            "template_engine": "handlebars",
            "markdown_pattern": "{{#if Thumbnail_URL}}[![Thumbnail]({{{Thumbnail_URL}}}){height=75}]({{{Thumbnail_URL}}}){{/if}}"
          },
          "name": "Thumbnail (click to view)",
          "detailed": {
            "template_engine": "handlebars",
            "markdown_pattern": "{{#if Thumbnail_URL}}[![Thumbnail]({{{Thumbnail_URL}}}){height=75}]({{{Thumbnail_URL}}}){{/if}}"
          }
        }
      }

    table_annotations = {
          "tag:isrd.isi.edu,2019:export": {
            "detailed": {
              "templates": [
                {
                  "type": "BAG",
                  "outputs": [
                    {
                      "source": {
                        "api": "attribute",
                        "path": "(RID)=(Imaging:Processed_Image:Reference_Image)/url:=File_URL,length:=File_Bytes,filename:=File_Name,md5:=File_MD5,Reference_Image"
                      },
                      "destination": {
                        "name": "Files/QuPath/{Reference_Image}",
                        "type": "fetch"
                      }
                    },
                    {
                      "source": {
                        "api": "attribute",
                        "path": "(RID)=(Imaging:Image_Z:Image)/url:=OME_Companion_URL,length:=OME_Companion_Bytes,filename:=OME_Companion_Name,md5:=OME_Companion_MD5,Image"
                      },
                      "destination": {
                        "name": "Files/QuPath/{Image}",
                        "type": "fetch"
                      }
                    },
                    {
                      "source": {
                        "api": "attribute",
                        "path": "(RID)=(Imaging:Image:Parent_Image)/(RID)=(Imaging:Processed_Image:Reference_Image)/url:=File_URL,length:=File_Bytes,filename:=File_Name,md5:=File_MD5,Reference_Image"
                      },
                      "destination": {
                        "name": "Files/QuPath/{Reference_Image}",
                        "type": "fetch"
                      }
                    },
                    {
                      "source": {
                        "api": "attribute",
                        "path": "(RID)=(Imaging:Image:Parent_Image)/(RID)=(Imaging:Image_Z:Image)/url:=OME_Companion_URL,length:=OME_Companion_Bytes,filename:=OME_Companion_Name,md5:=OME_Companion_MD5,Image"
                      },
                      "destination": {
                        "name": "Files/QuPath/{Image}",
                        "type": "fetch"
                      }
                    }
                  ],
                  "displayname": "BDBAG (all files)"
                }
              ]
            }
          },
        "tag:misd.isi.edu,2015:display": {
          "name": "Visualization Image"
        },
        "tag:isrd.isi.edu,2016:table-display": {
          "row_name": {
            "template_engine": "handlebars",
            "row_markdown_pattern": "{{{RID}}}: {{{Original_File_Name}}}"
          },
          "row_name/compact": {
            "template_engine": "handlebars",
            "row_markdown_pattern": "[:span: :/span:{.pseudo-column-rowname-thumbnail-title}![]({{#if Thumbnail_URL}}{{{Thumbnail_URL}}}{{/if}}){height=75}](/chaise/record/#{{{$catalog.snapshot}}}/Imaging:Image/RID={{{RID}}}){.pseudo-column-rowname-thumbnail-link}"
          }
        },
        "tag:isrd.isi.edu,2016:visible-columns": {
          "entry": [
            "RID",
            "Notes"
          ],
          "filter": {
            "and": [
              {
                "open": True,
                "source": [
                  {
                    "inbound": [
                      "Imaging",
                      "Image_Annotation_Image_fkey"
                    ]
                  },
                  {
                    "outbound": [
                      "Imaging",
                      "Image_Annotation_Anatomy_fkey"
                    ]
                  },
                  "RID"
                ],
                "markdown_name": "Annotated Anatomy"
              }
            ]
          },
          "compact": [
            "RID",
            {
              "source": "Pixels_Per_Meter"
            },
            {
              "sourcekey": "Annotated"
            },
            {
              "sourcekey": "Updated_Notes"
            }
          ],
          "detailed": [
            "RID",
            {
              "source": [
                {
                  "outbound": [
                    "Imaging",
                    "Image_Primary_Table_imaging_data_RID_fkey"
                  ]
                },
                "RID"
              ],
              "comment": "A reference to the primary image.",
              "markdown_name": "Primary Image"
            },
            {
              "source": "Pixels_Per_Meter"
            },
            {
              "source": "Notes"
            },
            {
              "source": "Thumbnail_URL"
            },
            {
              "source": "Default_Z",
              "markdown_name": "Displayed Z Index"
            },
            [
              "Imaging",
              "Image_Channel_Image_fkey"
            ],
            {
              "sourcekey": "Parent_Image_Row"
            },
            {
              "sourcekey": "Derived_Images"
            },
            {
              "source": [
                {
                  "inbound": [
                    "Imaging",
                    "Image_Annotation_Image_fkey"
                  ]
                },
                {
                  "outbound": [
                    "Imaging",
                    "Image_Annotation_Principal_Investigator_fkey"
                  ]
                },
                "RID"
              ],
              "comment": "PIs associated with annotations created for this image",
              "display": {
                "show_foreign_key_link": True
              },
              "aggregate": "array_d",
              "array_display": "csv",
              "markdown_name": "Image Annotators"
            },
            {
              "markdown_name": "uri",
              "hide_column_header": True,
              "display": {
                    "template_engine": "handlebars",
                    "markdown_pattern": "{{#if Generated_Zs}}::: iframe [](/chaise/viewer/#{{{$catalog.snapshot}}}/Imaging:Image/RID={{RID}}?waterMark=FaceBase{{#if _Pixels_Per_Meter}}&meterScaleInPixels={{_Pixels_Per_Meter}}{{/if}}){style=\"min-width:1000px; min-height:700px; height:80vh;\" class=chaise-autofill  } \n {{/if}}"
              }
            }
          ]
        },
        "tag:isrd.isi.edu,2019:source-definitions": {
          "fkeys": True,
          "columns": True,
          "sources": {
            "Annotated": {
              "source": [
                {
                  "inbound": [
                    "Imaging",
                    "Image_Annotation_Image_fkey"
                  ]
                },
                "RID"
              ],
              "comment": "Indicate whether the image has any annotations",
              "display": {
                "template_engine": "handlebars",
                "markdown_pattern": "{{#if (gt $_self 0)}}Yes{{/if}}"
              },
              "aggregate": "cnt",
              "markdown_name": "Annotated"
            },
            "Original_File": {
              "source": "Original_File_Name",
              "display": {
                "wait_for": [
                  "Num_Derived_Images"
                ],
                "template_engine": "handlebars",
                "markdown_pattern": "{{#if Parent_Image}}{{{Parent_Image_Row.values.Original_File_Name}}} (extracted image {{{Series}}}){{else if (gt Num_Derived_Images 0)}}{{{Original_File_Name}}} (image set of {{{Num_Derived_Images}}}){{else}}{{{Original_File_Name}}}{{/if}}"
              }
            },
            "Updated_Notes": {
              "source": "Notes",
              "display": {
                "wait_for": [
                  "Num_Derived_Images"
                ],
                "template_engine": "handlebars",
                "markdown_pattern": "{{{Notes}}} {{#with $fkey_Imaging_Image_Parent_Image_Image_RID_fkey}} {{#if Notes}}\n{{/if}} Extracted image {{{../Series}}} (from [{{{values.RID}}}]({{{uri.detailed}}})){{else if (gt _Num_Derived_Images 0) }} {{#if Notes}}\n{{/if}} Image set of {{{Num_Derived_Images}}} {{/with}}"
              }
            },
            "Derived_Images": {
              "source": [
                {
                  "inbound": [
                    "Imaging",
                    "Image_Parent_Image_Image_RID_fkey"
                  ]
                },
                "RID"
              ],
              "display": {
                "template_engine": "handlebars",
                "markdown_pattern": "The original file contains the following sequence of images. Click an individual image for visualization.\n\n {{#each $self}} [:span: Image {{{this.values.Series}}} :{{{this.values.RID}}} :/span:{.pseudo-column-rowname-thumbnail-title}![]({{#if this.values.Thumbnail_URL}}{{{this.values.Thumbnail_URL}}}{{else}}/facebase-images/click-for-image.png{{/if}}){height=150}]({{{this.uri.detailed}}}){.pseudo-column-rowname-thumbnail-link} {{/each}}"
              },
              "array_options": {
                "order": [
                  {
                    "column": "Series",
                    "descending": False
                  }
                ]
              },
              "markdown_name": "Extracted Images"
            },
            "Generated_Tiff": {
              "source": [
                {
                  "inbound": [
                    "Imaging",
                    "Processed_Image_Reference_Image_fkey"
                  ]
                },
                "RID"
              ],
              "array_options": {
                "order": [
                  {
                    "column": "Channel_Number",
                    "descending": False
                  }
                ]
              },
              "markdown_name": "Download TIFF"
            },
            "Parent_Image_Row": {
              "source": [
                {
                  "outbound": [
                    "Imaging",
                    "Image_Parent_Image_Image_RID_fkey"
                  ]
                },
                "RID"
              ],
              "display": {
                "template_engine": "handlebars",
                "markdown_pattern": "{{#if Parent_Image}}[{{{$self.values.RID}}}]({{{$self.uri.detailed}}}): {{{$self.values.Original_File_Name}}}{{/if}}"
              },
              "markdown_name": "Parent Image"
            },
            "Num_Derived_Images": {
              "source": [
                {
                  "inbound": [
                    "Imaging",
                    "Image_Parent_Image_Image_RID_fkey"
                  ]
                },
                "RID"
              ],
              "aggregate": "cnt",
              "markdown_name": "Number of Derived Images"
            }
          }
        },
        "tag:isrd.isi.edu,2016:visible-foreign-keys": {
          "*": []
        }
      }
 
    table_name = 'Image'
    comment = None
    model = catalog.getCatalogModel()
    schema = model.schemas[schema_name]
    if table_name not in schema.tables:
        column_defs = [
            Column.define(
                'Thumbnail_URL',
                builtin_types.text,
                comment=None, 
                annotations=thumbnail_annotations,                       
                nullok=True
                ),
            Column.define(
                'Notes',
                builtin_types.markdown,
                nullok=True
                ),
            Column.define(
                'Original_File_Name',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'Pixels_Per_Meter',
                builtin_types.int4,
                comment='Pixels per meter in image',
                nullok=True
                ),
            Column.define(
                'Parent_Image',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'Primary_Table',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'Default_Z',
                builtin_types.int4,
                comment='The default z index (depth) used for visualization',
                nullok=True
                ),
            Column.define(
                'Series',
                builtin_types.int4,
                comment='The series associated with this image in the original multi-image file. Null if the original file is a single image',
                nullok=True
                ),
            Column.define(
                'Total_Series',
                builtin_types.int4,
                comment='The total number of series associated with this image',
                nullok=True
                ),
            Column.define(
                'Download_Tiff_URL',
                builtin_types.text,
                comment='A tiff or ome-tiff image to be used for annotations using third party software such as QuPath. A tiff represents a single channel and single z image. A ome-tiff represents a multi-channels and single z image.',
                annotations = {
                    "tag:isrd.isi.edu,2017:asset": {
                      "browser_upload": False
                    }
                  },
                nullok=True
                ),
            Column.define(
                'Download_Tiff_Name',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'Download_Tiff_Bytes',
                builtin_types.int8,
                nullok=True
                ),
            Column.define(
                'Download_Tiff_MD5',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'Metadata_URL',
                builtin_types.text,
                comment='A succinct OME metadata file (JSON format) derived from the original OME XML metadata',
                nullok=True
                ),
            Column.define(
                'Metadata_Name',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'Metadata_Bytes',
                builtin_types.int8,
                nullok=True
                ),
            Column.define(
                'Metadata_MD5',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'OME_XML_URL',
                builtin_types.text,
                comment='OME XML metadata file',
                nullok=True
                ),
            Column.define(
                'OME_XML_Name',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'OME_XML_Bytes',
                builtin_types.int8,
                nullok=True
                ),
            Column.define(
                'OME_XML_MD5',
                builtin_types.text,
                nullok=True
                ),
            Column.define(
                'Properties',
                builtin_types.jsonb,
                nullok=True
                ),
            Column.define(
                'Generated_Zs',
                builtin_types.int4,
                comment='Number of z planes generated from this image.',
                nullok=True
                )
            ]

        key_defs = [
            Key.define(['RID', 'Default_Z'],
                       constraint_names=[['Imaging', 'Image_RID_Default_Z_key']]
            )
        ]
        fkey_defs = [
            ForeignKey.define(['Parent_Image'], 'Imaging', 'Image', ['RID'],
                              constraint_names=[['Imaging', 'Image_Parent_Image_Image_RID_fkey']],
                              on_update='CASCADE',
                              on_delete='CASCADE'   
            ),
            ForeignKey.define(['Primary_Table'], 'isa', 'imaging_data', ['RID'],
                              constraint_names=[['Imaging', 'Image_Primary_Table_imaging_data_RID_fkey']],
                              on_update='CASCADE',
                              on_delete='CASCADE'   
            )
        ]
        table_def = Table.define(
            table_name,
            column_defs,
            key_defs=key_defs,
            fkey_defs=fkey_defs,
            comment=comment,
            annotations=table_annotations,
            acls=table_acls,
            acl_bindings=image_acl_bindings,
            provide_system=True
        )
        
        schema.create_table(table_def)

def create_Imaging_schema_if_not_exists(catalog):
    annotations = {
        "tag:misd.isi.edu,2015:display": {
          "name_style": {
            "title_case": True,
            "underline_space": True
          }
        }
    }
    
    schema_name = 'Imaging'
    model = catalog.getCatalogModel()
    if schema_name not in model.schemas:
        schema_def = Schema.define(schema_name, acls=schema_acls, annotations=annotations)
        model.create_schema(schema_def)

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
print('Restoring ...')
restore(catalog_ermrest)

"""
Create new vocabulary tables.
"""
print('Creating vocabulary tables ...')
create_vocabulary_table_if_not_exist(catalog_ermrest, 'vocab', 'processing_status', 'A set of status for processing an image.')
create_vocabulary_table_if_not_exist(catalog_ermrest, 'vocab', 'display_method', 'Table containing controlled names for image display methods.')
create_vocabulary_table_if_not_exist(catalog_ermrest, 'vocab', 'color', 'Colors and other terms used to describe slide channel appearance.')

"""
Load data into the new vocabulary tables.
"""
print('Loading the vocabulary tables ...')
add_rows_to_vocab_processing_status(catalog_ermrest)
add_rows_to_vocab_display_method(catalog_ermrest)
add_rows_to_vocab_color(catalog_ermrest)

"""
Create the Imaging schema.
"""
print('Creating the Imaging schema ...')
create_Imaging_schema_if_not_exists(catalog_ermrest)

"""
Create the Image table.
"""
print('Creating the Image table ...')
create_image_table_if_not_exists(catalog_ermrest, 'Imaging')

"""
Create new tables required for the image processing.
"""
print('Creating the image tables ...')
create_image_z_table_if_not_exists(catalog_ermrest, 'Imaging')
create_image_channel_table_if_not_exists(catalog_ermrest, 'Imaging')
create_processed_image_table_if_not_exists(catalog_ermrest, 'Imaging')

"""
Create the image annotation tables.
"""
print('Creating the annotation tables ...')
create_image_annotation_file_table_if_not_exists(catalog_ermrest, 'Imaging')
create_image_annotation_table_if_not_exists(catalog_ermrest, 'Imaging')

"""
Add columns to the imaging_data table.
"""
print('Adding columns to the imaging_data table ...')
add_column_if_not_exist(catalog_ermrest, 'isa', 'imaging_data', 'processing_status', 'text', 'new', True)

"""
Create Foreign Keys.
"""
print('Adding FK to the imaging_data table ...')
create_foreign_key_if_not_exist(catalog_ermrest, 'isa', 'imaging_data', ['processing_status'], 'vocab', 'processing_status', ['Name'])

"""
Create visible annotations.
"""
print('Adding the source definition annotations ...')
add_annotation_source_definitions(catalog_ermrest, 'isa', 'imaging_data', source_definitions)

print('Adding the visible foreign keys annotations ...')
add_annotation_visible_foreign_keys(catalog_ermrest, 'isa', 'imaging_data', ['Imaging', 'Image_Primary_Table_imaging_data_RID_fkey'])

print('Adding the visible columns annotations ...')
add_annotation_visible_columns(catalog_ermrest, 'isa', 'imaging_data', ['isa', 'imaging_data_processing_status_fkey'])
add_annotation_visible_columns(catalog_ermrest, 'isa', 'imaging_data', virtual_column)

print('End of schema updates')

