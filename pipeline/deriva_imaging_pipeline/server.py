#!/usr/bin/python3

import os
import json
from deriva.core import PollingErmrestCatalog, init_logging
import subprocess
import logging
import socket
import sys
import traceback
import argparse
from .worker import DerivaImagingWorker
from .client import get_configuration


# Loglevel dictionary
__LOGLEVEL = {
    'critical': logging.FATAL,
    'fatal': logging.CRITICAL,
    'error': logging.ERROR,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG
}

FORMAT = '%(asctime)s: %(levelname)s <%(module)s>: %(message)s'

logger = logging.getLogger(__name__)
config = None
deriva_worker_configuration = None

class WorkerRuntimeError (RuntimeError):
    pass

class WorkerBadDataError (RuntimeError):
    pass

class WorkUnit (object):
    def __init__(
            self,
            get_claimable_url,
            put_claim_url,
            put_update_baseurl,
            run_row_job,
            claim_input_data=lambda row: {'RID': row['RID'], config['image_processing_status']: "in progress"},
            failure_input_data=lambda row, e: {'RID': row['RID'], config['image_processing_status']: "error"}
    ):
        self.get_claimable_url = get_claimable_url
        self.put_claim_url = put_claim_url
        self.put_update_baseurl = put_update_baseurl
        self.run_row_job = run_row_job
        self.claim_input_data = claim_input_data
        self.failure_input_data = failure_input_data
        self.idle_etag = None

_work_units = []

def image_row_job(handler):
    """
    Generate a tiled pyramid from a tiff file of the original Image table.
    """
    tiff_row_job(handler)
    
def tiff_row_job(handler):
    """
    Run the script for generating jpg image.
    """
    
    try:
        row = handler.row
        unit = handler.unit
        
        logger.info('Running job for generating a tiled pyramid for RID="%s" and Filename="%s".' % (row['RID'], row[config['original_file_name']])) 
        deriva_imaging_worker = DerivaImagingWorker(deriva_worker_configuration)
        returncode = deriva_imaging_worker.processImage(row['RID'])
    except:
        et, ev, tb = sys.exc_info()
        logger.error('got unexpected exception "%s"' % str(ev))
        logger.error('%s' % ''.join(traceback.format_exception(et, ev, tb)))
        returncode = 1
        
    if returncode != 0:
        logger.error('Could not execute the worker script') 
        raise WorkerRuntimeError('Could not execute the worker script')
    else:
        logger.info('Finished job for generating jpg image for RID="%s" and Filename="%s".' % (row['RID'], row[config['original_file_name']])) 
                

class Worker (object):

    def __init__(self, row, unit):
        logger.info('Claimed job %s.\n' % row.get('RID'))

        self.row = row
        self.unit = unit

    work_units = _work_units # these are defined above w/ their funcs and URLs...

    @classmethod
    def look_for_work(cls):
        """Find, claim, and process work for each work unit.

        Do find/claim with HTTP opportunistic concurrency control and
        caching for efficient polling and quiescencs.

        On error, set Processing_Status="failed: reason"

        Result:
         true: there might be more work to claim
         false: we failed to find any work
        """
        found_work = False

        for unit in cls.work_units:
            # this handled concurrent update for us to safely and efficiently claim a record
            try:
                unit.idle_etag, batch = cls.catalog.state_change_once(
                    unit.get_claimable_url,
                    unit.put_claim_url,
                    unit.claim_input_data,
                    unit.idle_etag
                )
            except:
                # keep going if we have a broken WorkUnit
                continue
            # batch may be empty if no work was found...
            for row, claim in batch:
                found_work = True
                try:
                    handler = cls(row, unit)
                    unit.run_row_job(handler)
                except WorkerBadDataError as e:
                    logger.error("Aborting task %s on data error: %s\n" % (row["RID"], e))
                    cls.catalog.put(unit.put_claim_url, json=[unit.failure_input_data(row, e)])
                    # continue with next task...?
                except WorkerRuntimeError as e:
                    logger.error("Aborting task %s on data error: %s\n" % (row["RID"], e))
                    cls.catalog.put(unit.put_claim_url, json=[unit.failure_input_data(row, e)])
                    # continue with next task...?
                except Exception as e:
                    cls.catalog.put(unit.put_claim_url, json=[unit.failure_input_data(row, e)])
                    raise

        return found_work

    @classmethod
    def blocking_poll(cls):
        return cls.catalog.blocking_poll(cls.look_for_work, polling_seconds=cls.poll_seconds)

def main():
    global config, logger, deriva_worker_configuration, _work_units
    
    parser = argparse.ArgumentParser(description='Tool to process deriva images.')
    parser.add_argument( '--config', action='store', type=str, help='The JSON configuration file.', required=True)
    args = parser.parse_args()
    with open(args.config, 'r') as f:
        config = json.load(f)
    loglevel = config.get('loglevel', None)
    if loglevel:
        loglevel = __LOGLEVEL.get(loglevel, None)
    logfile = config.get('log', None)
    if loglevel and logfile:
        init_logging(level=loglevel, log_format=FORMAT, file_path=logfile)
    else:
        logging.getLogger().addHandler(logging.NullHandler())
    deriva_worker_configuration = get_configuration(config, logger)
    if deriva_worker_configuration == None:
        return 1
    _work_units.append(
        WorkUnit(
            config['get_claimable_url'],
            config['put_claim_url'],
            config['put_update_baseurl'],
            image_row_job
        )
    )
    
    servername = config['deriva_imaging_server']

    # secret session cookie
    credfile = config['credentials_file']
    credentials = json.load(open(credfile))
    if 'cookie' not in credentials.keys():
        credentials = credentials[servername]


    # these are peristent/logical connections so we create once and reuse
    # they can retain state and manage an actual HTTP connection-pool
    catalog = PollingErmrestCatalog(
        'https', 
        servername,
        config['catalog_number'],
        credentials
    )
    catalog.dcctx['cid'] = 'pipeline/image/2D/tiff'
    Worker.poll_seconds = int(os.getenv('DERIVA_IMAGING_POLL_SECONDS', '300'))
    Worker.catalog = catalog
    Worker.blocking_poll()
    return 0


if __name__ == '__main__':
    sys.exit(main())

