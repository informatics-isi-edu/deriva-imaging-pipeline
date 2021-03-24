#!/usr/bin/python3

# 
# Copyright 2017 University of Southern California
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Load configuration for the tiled pyramid Generator from tiff.
Check for tiff images.
Update the ermrest tables.
"""

import os
import logging
import json
import sys
import traceback

from bioformats_processing_lib import DerivaImagingClient
from deriva.core import init_logging

FORMAT = '%(asctime)s: %(levelname)s <%(module)s>: %(message)s'
logger = logging.getLogger(__name__)

# Loglevel dictionary
__LOGLEVEL = {'error': logging.ERROR,
              'warning': logging.WARNING,
              'info': logging.INFO,
              'debug': logging.DEBUG}

def load(config_filename):
    """
    Read the configuration file.
    """
    
    # Load configuration file, or create configuration based on arguments
    cfg = {}
    if os.path.exists(config_filename):
        f = open(config_filename, 'r')
        try:
            cfg = json.load(f)
            loglevel = cfg.get('loglevel', None)
            logfile = cfg.get('log', None)
            if loglevel and logfile:
                init_logging(level=__LOGLEVEL.get(loglevel), log_format=FORMAT, file_path=logfile)
            else:
                logging.getLogger().addHandler(logging.NullHandler())
            logger.debug("config: %s" % cfg)
        except ValueError as e:
            logger.error('Malformed configuration file: %s' % e)
            return None
        else:
            f.close()
    else:
        sys.stderr.write('Configuration file: "%s" does not exist.\n' % config_filename)
        return None
    
    # Ermrest settings
    url = os.getenv('URL', None)
    if url == None:
        logger.error('URL must be supplied through the "URL" environment variable.')
        logger.error('Launch the script "env URL=https://foo.org/ermrest/catalog/N bioformats_processing.py --config <config-file>".')
        return None
        
    logger.info('URL: %s' % url)
    
    bioformat_server = cfg.get('bioformat_server', None)
    if bioformat_server == None:
        logger.error('The bioformat server must be supplied.')
        logger.error('Launch the script "env URL=https://foo.org/ermrest/catalog/N bioformats_processing.py --config <config-file>".')
        return None

    credfile = cfg['credentials_file']
    cookie = json.load(open(credfile))
    if 'cookie' not in cookie.keys():
        cookie = cookie[bioformat_server]['cookie']

    if not cookie:
        logger.error('Ermrest cookie must be provided.')
        return None

    hatrac_template = cfg.get('hatrac_template', None)
    if hatrac_template == None:
        logger.error('Hatrac template must be provided.')
        return None

    iiif_url = cfg.get('iiif_url', None)
    if iiif_url == None:
        logger.error('IIIF URL must be provided.')
        return None

    data_scratch = cfg.get('data_scratch', None)
    if not data_scratch or not os.path.isdir(data_scratch):
        logger.error('data_scratch directory must be provided and exists.')
        return None

    curl = cfg.get('curl', None)
    if not curl or not os.path.isfile(curl):
        logger.error('curl application must be provided and exists.')
        return None

    wget = cfg.get('wget', None)
    if not wget or not os.path.isfile(wget):
        logger.error('wget application must be provided and exists.')
        return None

    images = cfg.get('images', None)
    if not images or not os.path.isdir('/var/www/html/%s' % images):
        logger.error('images directory must be provided and exists.')
        return None

    output_metadata = cfg.get('output_metadata', None)
    if not output_metadata or not os.path.isdir('/var/www/html/%s' % output_metadata):
        logger.error('output_metadata directory must be provided and exists.')
        return None

    extract_scenes = cfg.get('extract_scenes', None)
    if not extract_scenes or not os.path.isfile(extract_scenes):
        logger.error('extract_scenes.py application must be given and exist.')
        return None

    model_file = cfg.get('model_file', None)
    if not model_file or not os.path.isfile(model_file):
        logger.error('model file must be given and exist.')
        return None

    with open(model_file, 'r') as fr:
        model = json.load(fr)
    
    python_app = cfg.get('python', None)
    if not python_app or not os.path.isfile(python_app):
        logger.error('python3 application must be given and exist.')
        return None

    tiffinfo = cfg.get('tiffinfo', None)
    if not tiffinfo or not os.path.isfile(tiffinfo):
        logger.error('tiffinfo application must be given and exist.')
        return None

    viewer = cfg.get('viewer', None)
    if not viewer:
        logger.error('The viewer application must be given.')
        return None

    version = cfg.get('version', 'v1.0')
    mail_server = cfg.get('mail_server', None)
    mail_sender = cfg.get('mail_sender', None)
    mail_receiver = cfg.get('mail_receiver', None)
    mail_file = cfg.get('mail_file', None)

    # Establish Ermrest client connection
    try:
        client = DerivaImagingClient(baseuri=url, \
                               version=version, \
                               cookie=cookie, \
                               data_scratch=data_scratch, \
                               images=images, \
                               output_metadata=output_metadata, \
                               curl=curl, \
                               wget=wget, \
                               tiffinfo=tiffinfo, \
                               extract_scenes=extract_scenes, \
                               python_app=python_app, \
                               model=model, \
                               hatrac_template=hatrac_template, \
                               iiif_url=iiif_url, \
                               viewer=viewer, \
                               mail_server=mail_server, \
                               mail_sender=mail_sender, \
                               mail_receiver=mail_receiver,
                               mail_file=mail_file,
                               logger=logger)
    except:
        et, ev, tb = sys.exc_info()
        logger.error('got INIT exception "%s"' % str(ev))
        logger.error('%s' % ''.join(traceback.format_exception(et, ev, tb)))
        return None
    
    return client

try:
    if len(sys.argv) < 3:
        raise
    config_filename = sys.argv[2]
    client = load(config_filename)
    if client:
        try:
            client.start()
        except:
            sys.exit(1)
except SystemExit:
    pass
except:
    et, ev, tb = sys.exc_info()
    sys.stderr.write('got exception "%s"' % str(ev))
    sys.stderr.write('%s' % ''.join(traceback.format_exception(et, ev, tb)))
    sys.stderr.write('\nusage: env URL=https://foo.org/ermrest/catalog/N bioformats_processing.py --config <config-file>\n\n')
    sys.exit(1)

