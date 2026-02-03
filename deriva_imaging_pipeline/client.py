#!/usr/bin/python
"""DERIVA Imaging Pipeline client for processing individual images.

This module provides the CLI client for processing single images by RID.
It loads configuration, initializes the worker, and processes the specified image.

Example:
    Command line usage::

        $ deriva-imaging-client --config ~/.deriva_imaging.json --rid 1-ABCD

    Python usage::

        from deriva_imaging_pipeline.client import load, get_configuration
        cfg = load("config.json")
        config = get_configuration(cfg, logger)
"""

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

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import traceback
from typing import Any, Optional

from deriva.core import init_logging

from .worker import DerivaImagingWorker

FORMAT = '%(asctime)s: %(levelname)s <%(module)s>: %(message)s'
logger = logging.getLogger(__name__)

# Loglevel dictionary mapping string names to logging constants
_LOGLEVEL: dict[str, int] = {
    'critical': logging.FATAL,
    'fatal': logging.CRITICAL,
    'error': logging.ERROR,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG
}


def load(config_filename: str) -> Optional[dict[str, Any]]:
    """Load and parse a JSON configuration file.

    Reads the configuration file and initializes logging based on the
    'loglevel' and 'log' settings in the configuration.

    Args:
        config_filename: Path to the JSON configuration file.

    Returns:
        Dictionary containing the configuration, or None if the file
        doesn't exist or is malformed.

    Example:
        >>> cfg = load("/path/to/config.json")
        >>> if cfg:
        ...     print(cfg['baseuri'])
    """
    if not os.path.exists(config_filename):
        sys.stderr.write(f'Configuration file: "{config_filename}" does not exist.\n')
        return None

    try:
        with open(config_filename, 'r') as f:
            cfg: dict[str, Any] = json.load(f)

        loglevel = cfg.get('loglevel')
        logfile = cfg.get('log')

        if loglevel and logfile:
            init_logging(level=_LOGLEVEL.get(loglevel), log_format=FORMAT, file_path=logfile)
        else:
            logging.getLogger().addHandler(logging.NullHandler())

        logger.debug("config: %s", cfg)
        return cfg

    except ValueError as e:
        logger.error('Malformed configuration file: %s', e)
        return None


def get_configuration(cfg: dict[str, Any], log: logging.Logger) -> Optional[dict[str, Any]]:
    """Extract and validate worker configuration from raw config.

    Validates that all required configuration fields are present and that
    referenced files and directories exist.

    Args:
        cfg: Raw configuration dictionary from load().
        log: Logger instance for error reporting.

    Returns:
        Validated configuration dictionary suitable for DerivaImagingWorker,
        or None if validation fails.

    Required configuration fields:
        - baseuri: Base URI for the DERIVA catalog
        - deriva_imaging_server: Hostname of the imaging server
        - credentials_file: Path to credentials JSON file
        - hatrac_template: Template for Hatrac object paths
        - iiif_url: Base URL for IIIF image server
        - data_scratch: Directory for temporary processing files
        - curl: Path to curl executable
        - wget: Path to wget executable
        - images: Subdirectory under /var/www/html for images
        - output_metadata: Subdirectory for output metadata
        - model_file: Path to model JSON file
        - python: Path to Python interpreter
        - tiffinfo: Path to tiffinfo executable
        - viewer: Viewer application path
    """
    config: dict[str, Any] = {}

    # Required: baseuri
    baseuri = cfg.get('baseuri')
    if baseuri is None:
        log.error('The "baseuri" must be supplied in the configuration file.')
        return None
    config['baseuri'] = baseuri

    # Required: deriva_imaging_server
    deriva_imaging_server = cfg.get('deriva_imaging_server')
    if deriva_imaging_server is None:
        log.error('The "deriva_imaging_server" must be supplied in the configuration file.')
        return None

    # Required: credentials_file
    credfile = cfg.get('credentials_file')
    if not credfile or not os.path.isfile(credfile):
        log.error('The "credentials_file" must be provided in the configuration file and exist.')
        return None

    cookie = json.load(open(credfile))
    if 'cookie' not in cookie.keys():
        cookie = cookie[deriva_imaging_server]['cookie']

    if not cookie:
        log.error('The ermrest cookie could not be identified.')
        return None
    config['cookie'] = cookie

    # Required: hatrac_template
    hatrac_template = cfg.get('hatrac_template')
    if hatrac_template is None:
        log.error('The "hatrac_template" must be provided in the configuration file.')
        return None
    config['hatrac_template'] = hatrac_template

    # Required: iiif_url
    iiif_url = cfg.get('iiif_url')
    if iiif_url is None:
        log.error('The "iiif_url" must be provided in the configuration file.')
        return None
    config['iiif_url'] = iiif_url

    # Required: data_scratch
    data_scratch = cfg.get('data_scratch')
    if not data_scratch or not os.path.isdir(data_scratch):
        log.error('The "data_scratch" directory must be provided in the configuration file and exist.')
        return None
    config['data_scratch'] = data_scratch

    # Required: curl
    curl = cfg.get('curl')
    if not curl or not os.path.isfile(curl):
        log.error('The "curl" application must be provided in the configuration file and exist.')
        return None
    config['curl'] = curl

    # Required: wget
    wget = cfg.get('wget')
    if not wget or not os.path.isfile(wget):
        log.error('The "wget" application must be provided in the configuration file and exist.')
        return None
    config['wget'] = wget

    # Required: images directory
    images = cfg.get('images')
    if not images or not os.path.isdir(f'/var/www/html/{images}'):
        log.error('The "images" directory must be provided in the configuration file and exist.')
        return None
    config['images'] = images

    # Required: output_metadata directory
    output_metadata = cfg.get('output_metadata')
    if not output_metadata or not os.path.isdir(f'/var/www/html/{output_metadata}'):
        log.error('The "output_metadata" directory must be provided in the configuration file and exist.')
        return None
    config['output_metadata'] = output_metadata

    # Optional: processing_dir
    config['processing_dir'] = cfg.get('processing_dir')

    # Required: model_file
    model_file = cfg.get('model_file')
    if not model_file or not os.path.isfile(model_file):
        log.error('The "model_file" must be provided in the configuration file and exist.')
        return None

    with open(model_file, 'r') as fr:
        model = json.load(fr)
    config['model'] = model

    # Required: python
    python_app = cfg.get('python')
    if not python_app or not os.path.isfile(python_app):
        log.error('The "python" application must be provided in the configuration file and exist.')
        return None
    config['python_app'] = python_app

    # Required: tiffinfo
    tiffinfo = cfg.get('tiffinfo')
    if not tiffinfo or not os.path.isfile(tiffinfo):
        log.error('The "tiffinfo" application must be provided in the configuration file and exist.')
        return None
    config['tiffinfo'] = tiffinfo

    # Required: viewer
    viewer = cfg.get('viewer')
    if not viewer:
        log.error('The "viewer" application must be provided in the configuration file.')
        return None
    config['viewer'] = viewer

    # Optional with default: version
    config['version'] = cfg.get('version', 'v1.0')

    # Optional: mail settings
    config['mail_server'] = cfg.get('mail_server')
    config['mail_sender'] = cfg.get('mail_sender')
    config['mail_receiver'] = cfg.get('mail_receiver')
    config['mail_file'] = cfg.get('mail_file')

    config['logger'] = log

    return config


def main() -> int:
    """CLI entry point for deriva-imaging-client.

    Parses command line arguments and processes a single image by RID.

    Returns:
        0 on success, 1 on error.
    """
    parser = argparse.ArgumentParser(description='Tool to process DERIVA images by RID.')
    parser.add_argument('--config', action='store', type=str,
                        help='The JSON configuration file.', required=True)
    parser.add_argument('--rid', action='store', type=str,
                        help='The RID of the image to process.', required=True)
    args = parser.parse_args()

    try:
        config = load(args.config)
        if config is None:
            return 1

        deriva_worker_configuration = get_configuration(config, logger)
        if deriva_worker_configuration is None:
            return 1

        try:
            deriva_imaging_worker = DerivaImagingWorker(deriva_worker_configuration)
            return_status = deriva_imaging_worker.processImage(args.rid)
            logger.debug('Return Status: %s', return_status)
            return return_status
        except Exception:
            et, ev, tb = sys.exc_info()
            sys.stderr.write(f'got exception "{ev}"\n')
            sys.stderr.write(''.join(traceback.format_exception(et, ev, tb)))
            sys.stderr.write('\nusage: deriva-imaging-client --config <config-file> --rid <rid>\n\n')
            return 1

    except Exception:
        et, ev, tb = sys.exc_info()
        sys.stderr.write(f'got exception "{ev}"\n')
        sys.stderr.write(''.join(traceback.format_exception(et, ev, tb)))
        sys.stderr.write('\nusage: deriva-imaging-client --config <config-file> --rid <rid>\n\n')
        return 1


if __name__ == '__main__':
    sys.exit(main())
