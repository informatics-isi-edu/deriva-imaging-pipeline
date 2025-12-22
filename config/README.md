# Configuration

The pipeline requires configuration files with the following parameters. Examples are provided in this directory.

## `deriva_imaging.json`

Service configuration parameters.

```json
{
    "baseuri": "base URI of ERMrest catalog",
    "python": "path to python3",
    "processing_dir": "path to temporary processing directory",
    "data_scratch": "path to temporary data directory",
    "curl": "path to curl binary",
    "wget": "path to wget binary",
    "tiffinfo": "path to tiffinfo binary",
    "version": "v1.1",
    "images": "subdirectory under `/var/www/html` for processing OME image data",
    "output_metadata": "subdirectory under `/var/www/html` for processing OME image metadata",
    "viewer": "openseadragon-viewer viewer app html page",
    "log": "path to save logs",
    "loglevel": "log level, e.g., 'debug'",
    "image_processing_status": "processing status flag -- should be part of model.json?",
    "original_file_name": "filename column name -- should be part of model.json?",
    "get_claimable_url": "ERMrest path URL to get claimable work",
    "put_claim_url": "ERMrest path URL to set claimable work",
    "put_update_baseurl": "ERMrest path URL to put processed image record",
    "deriva_imaging_server": "hostname of imaging server",
    "credentials_file": "relative path to worker credentials",
    "catalog_number": "catalog identifier",
    "hatrac_template": "relative URL template for HATRAC storage location",
    "iiif_url": "IIIF service URL",
    "model_file": "path to model.json configuration file",
    "mail_server": "mail server",
    "mail_sender": "mail sender email address",
    "mail_receiver": "mail receiver",
    "mail_file": "path to mail file"
}
```

## `model.json`

Model mapping configuration.

```json
{
    "primary_schema": "schema name of primary file table",
    "primary_table": "table name of primary file table",
    "primary_file_name": "filename column name",
    "primary_file_url": "url column name",
    "primary_file_bytes": "byte_count column name",
    "primary_file_md5": "md5 column name",
    "primary_file_thumbnail": "schema:table/column name for primary file thumbnail url",
    "processing_status": "processing_status column name",
    "image_schema": "schema name of processed image table",
    "image_table": "table name of processed image table",
    "processed_image": "Processed_Image column name",
    "image_channel": "Image_Channel column name",
    "image_z": "Image_Z column name"
}
```