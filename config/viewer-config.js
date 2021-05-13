var viewerConfigs = {
    "*": {
        "image": {
            /**
             * the z-index of displayed main image. if the z-plane image doesn't change,
             * this value will be used for fetching and storing the annotations.
             */
            "default_z_index_column_name": "Default_Z",

            /**
             * the column that has the pixel per meter value. If value is defined,
             * it will be passed without any modifications to openseadragon-viewer
             */
            "pixel_per_meter_column_name": "Pixels_Per_Meter",

            // what should be displayed in the head title (the browser tab)
            "head_title_markdown_pattern": "",

            // what should be displayed in the page title in full screen mode (non-iframe)
            "page_title_markdown_pattern": "",
        },
        /**
         * the table that stores each individual image for each channel in each z-plane
         */
        "processed_image": {
            // procesed image table
            "schema_name": "Imaging",
            "table_name": "Processed_Image",

            // how to sort the processed_image records
            "column_order": [{
                "column": "Z_Index",
                "descending": false
            }, {
                "column": "Channel_Number",
                "descending": false
            }],

            // where to filter based on z_index and the image
            "z_index_column_name": "Z_Index",
            "reference_image_column_name": "Reference_Image",

            // the actual location of image file (info.json, ImageProperties.xml, etc)
            "image_url_column_name": "File_URL",
            
            // the channel number column
            "channel_number_column_name": "Channel_Number",

            // what is the display method (`iiif`, `dzi`, etc)
            "display_method_column_name": "Display_Method",

            // how to generate the url
            "iiif_version": "2", // if not passed, `2` will be used
            "image_url_pattern": {
                "iiif": "/iiif/{{{iiif_version}}}/{{#encode _url}}{{/encode}}/info.json"
            }
        },
        /**
         * the table that stores the channel data
         */
        "image_channel": {
            // channel table
            "schema_name": "Imaging",
            "table_name": "Image_Channel",

            // the sort criteria
            "column_order": [{
                "column": "Channel_Number",
                "descending": false
            }],

            // the fk column to image
            "reference_image_column_name": "Image",

            // the channelName column
            "channel_name_column_name": "Name",
            
            // the channelNumber column
            "channel_number_column_name": "Channel_Number",

            // the pseudoColor column (the value must be in color hex format)
            "pseudo_color_column_name": "Pseudo_Color",

            // a boolean column that signals whether the image is greyscale or rgb
            "is_rgb_column_name": "Is_RGB",

            /**
             * @DEPRECATED This is here for just backward compatibilty and should not be used.
             * the actual location of image file (info.json, ImageProperties.xml, etc)
             */
            "image_url_column_name": "Image_URL"
        },
        /**
         * the table that stores the annotation data
         */
        "image_annotation": {
            // annotation table
            "schema_name": "Imaging",
            "table_name": "Image_Annotation",

            // fk to image table in annotation table
            "reference_image_visible_column_name": "hlfOY0hhIprg_cr6yKd1Tg",
            "reference_image_column_name": "Image",

            // the asset column that has the annotation
            "overlay_column_name": "File_URL",

            // the columns that are used internally and should be removed from the entry form
            // TODO should be improved
            "z_index_column_name": "Z_Index",
            "channels_column_name": "Channels",

            /**
             * annoated term fk in annotation table
             * This information is used to ensure the combination of image, annotated_term, z_index are unique
             */
            "annotated_term_displayname": "Anatomy",
            "annotated_term_column_name": "Anatomy",
            "annotated_term_visible_column_name": "SyZB2C2sWgwedJI5WWFOYQ",
            "annotated_term_foreign_key_constraint": ["Imaging", "Image_Annotation_Anatomy_fkey"],

            /**
             * annoated term table
             * This information is used to display proper value (id vs name)
             */
            "annotated_term_table_name": "anatomy",
            "annotated_term_table_schema_name": "vocab",
            "annotated_term_id_column_name": "id",
            "annotated_term_name_column_name": "name"
        }
    }
};

