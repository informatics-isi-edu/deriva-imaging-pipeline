#!/usr/bin/python

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
import argparse

from .worker import DerivaImagingWorker
from deriva.core import init_logging

FORMAT = '%(asctime)s: %(levelname)s <%(module)s>: %(message)s'
logger = logging.getLogger(__name__)

# Loglevel dictionary
__LOGLEVEL = {
    'critical': logging.FATAL,
    'fatal': logging.CRITICAL,
    'error': logging.ERROR,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG
}

def load(config_filename):
    """
    Read the configuration file.
    """
    
    # Load configuration file, or create configuration based on arguments
    cfg = {}
    if os.path.exists(config_filename):
        try:
            with open(config_filename, 'r') as f:
                cfg = json.load(f)
            loglevel = cfg.get('loglevel', None)
            logfile = cfg.get('log', None)
            if loglevel and logfile:
                init_logging(level=__LOGLEVEL.get(loglevel), log_format=FORMAT, file_path=logfile)
            else:
                logging.getLogger().addHandler(logging.NullHandler())
            logger.debug("config: %s" % cfg)
            return cfg
        except ValueError as e:
            logger.error('Malformed configuration file: %s' % e)
            return None
    else:
        sys.stderr.write('Configuration file: "%s" does not exist.\n' % config_filename)
        return None
    
    
def get_configuration(cfg, logger):
    """
    Return the client configuration.
    """
    
    config = {}
    baseuri = cfg.get('baseuri', None)
    if baseuri == None:
        logger.error('The "baseuri" must be supplied in the configuration file.')
        return None
        
    config['baseuri'] = baseuri
    
    deriva_imaging_server = cfg.get('deriva_imaging_server', None)
    if deriva_imaging_server == None:
        logger.error('The "deriva_imaging_server" must be supplied in the configuration file.')
        return None

    credfile = cfg.get('credentials_file', None)
    if not credfile or not os.path.isfile(credfile):
        logger.error('The "credendential_file" must be provided in the configuration file and exists.')
        return None

    cookie = json.load(open(credfile))
    if 'cookie' not in cookie.keys():
        cookie = cookie[deriva_imaging_server]['cookie']

    if not cookie:
        logger.error('The ermrest cookie could not be identified.')
        return None

    config['cookie'] = cookie

    hatrac_template = cfg.get('hatrac_template', None)
    if hatrac_template == None:
        logger.error('The "hatrac_template" must be provided in the configuration file.')
        return None

    config['hatrac_template'] = hatrac_template

    iiif_url = cfg.get('iiif_url', None)
    if iiif_url == None:
        logger.error('The "iiif_url" must be provided in the configuration file.')
        return None

    config['iiif_url'] = iiif_url

    data_scratch = cfg.get('data_scratch', None)
    if not data_scratch or not os.path.isdir(data_scratch):
        logger.error('The "data_scratch" directory must be provided in the configuration file and exists.')
        return None

    config['data_scratch'] = data_scratch

    curl = cfg.get('curl', None)
    if not curl or not os.path.isfile(curl):
        logger.error('The "curl" application must be provided in the configuration file and exists.')
        return None

    config['curl'] = curl

    wget = cfg.get('wget', None)
    if not wget or not os.path.isfile(wget):
        logger.error('The "wget" application must be provided in the configuration file and exists.')
        return None

    config['wget'] = wget

    images = cfg.get('images', None)
    if not images or not os.path.isdir('/var/www/html/%s' % images):
        logger.error('The "images" directory must be provided in the configuration file and exists.')
        return None

    config['images'] = images

    output_metadata = cfg.get('output_metadata', None)
    if not output_metadata or not os.path.isdir('/var/www/html/%s' % output_metadata):
        logger.error('The "output_metadata" directory must be provided in the configuration file and exists.')
        return None

    config['output_metadata'] = output_metadata

    processing_dir = cfg.get('processing_dir', None)
    config['processing_dir'] = processing_dir

    model_file = cfg.get('model_file', None)
    if not model_file or not os.path.isfile(model_file):
        logger.error('The "model_file" must be provided in the configuration file and exist.')
        return None

    with open(model_file, 'r') as fr:
        model = json.load(fr)
    
    config['model'] = model

    python_app = cfg.get('python', None)
    if not python_app or not os.path.isfile(python_app):
        logger.error('The "python" application must be provided in the configuration file and exist.')
        return None

    config['python_app'] = python_app

    tiffinfo = cfg.get('tiffinfo', None)
    if not tiffinfo or not os.path.isfile(tiffinfo):
        logger.error('The "tiffinfo" application must be provided in the configuration file and exist.')
        return None

    config['tiffinfo'] = tiffinfo

    viewer = cfg.get('viewer', None)
    if not viewer:
        logger.error('The "viewer" application must be provided in the configuration file.')
        return None

    config['viewer'] = viewer

    version = cfg.get('version', 'v1.0')
    config['version'] = version

    mail_server = cfg.get('mail_server', None)
    config['mail_server'] = mail_server

    mail_sender = cfg.get('mail_sender', None)
    config['mail_sender'] = mail_sender

    mail_receiver = cfg.get('mail_receiver', None)
    config['mail_receiver'] = mail_receiver

    mail_file = cfg.get('mail_file', None)
    config['mail_file'] = mail_file

    config['logger'] = logger
    
    return config

def main():
    parser = argparse.ArgumentParser(description='Tool to process deriva images.')
    parser.add_argument( '--config', action='store', type=str, help='The JSON configuration file.', required=True)
    parser.add_argument( '--rid', action='store', type=str, help='The RID of the parent table.', required=True)
    args = parser.parse_args()
    
    try:
        config = load(args.config)
        if config != None:
            deriva_worker_configuration = get_configuration(config, logger)
            if deriva_worker_configuration != None:
                try:
                    deriva_imaging_worker = DerivaImagingWorker(deriva_worker_configuration)
                    returnStatus = deriva_imaging_worker.processImage(args.rid)
                    logger.debug('Return Status: {}'.format(returnStatus))
                    return returnStatus
                except:
                    et, ev, tb = sys.exc_info()
                    sys.stderr.write('got exception "%s"' % str(ev))
                    sys.stderr.write('%s' % ''.join(traceback.format_exception(et, ev, tb)))
                    sys.stderr.write('\nusage: deriva-imaging-client --config <config-file> --rid <rid>\n\n')
                    return 1
    except:
        et, ev, tb = sys.exc_info()
        sys.stderr.write('got exception "%s"' % str(ev))
        sys.stderr.write('%s' % ''.join(traceback.format_exception(et, ev, tb)))
        sys.stderr.write('\nusage: deriva-imaging-client --config <config-file> --rid <rid>\n\n')
        return 1


if __name__ == '__main__':
    sys.exit(main())

