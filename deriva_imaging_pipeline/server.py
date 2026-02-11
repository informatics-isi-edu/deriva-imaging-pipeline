#!/usr/bin/python
"""DERIVA Imaging Pipeline polling server.

This module provides a long-running server that polls the DERIVA catalog
for new images to process. It claims work items, processes them using
DerivaImagingWorker, and updates the catalog with results.

Example:
    Command line usage::

        $ export DERIVA_IMAGING_POLL_SECONDS=300
        $ deriva-imaging-server --config ~/.deriva_imaging.json

Environment Variables:
    DERIVA_IMAGING_POLL_SECONDS: Polling interval in seconds (default: 300)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import traceback
from typing import Any, Callable, Optional

from deriva.core import PollingErmrestCatalog, init_logging

from .client import get_configuration
from .worker import DerivaImagingWorker

# Loglevel dictionary mapping string names to logging constants
_LOGLEVEL: dict[str, int] = {
    'critical': logging.FATAL,
    'fatal': logging.CRITICAL,
    'error': logging.ERROR,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG
}

FORMAT = '%(asctime)s: %(levelname)s <%(module)s>: %(message)s'

logger = logging.getLogger(__name__)

# Global configuration (set in main())
config: Optional[dict[str, Any]] = None
deriva_worker_configuration: Optional[dict[str, Any]] = None


class WorkerRuntimeError(RuntimeError):
    """Exception raised when worker execution fails."""
    pass


class WorkerBadDataError(RuntimeError):
    """Exception raised when input data is invalid."""
    pass


class WorkUnit:
    """Defines a unit of work with URLs and handlers for claim/update operations.

    A WorkUnit encapsulates the URLs and callback functions needed to:
    - Find claimable work items
    - Claim work items for processing
    - Update work items after processing
    - Handle failures

    Attributes:
        get_claimable_url: URL path to query for claimable work items.
        put_claim_url: URL path to claim a work item.
        put_update_baseurl: Base URL path for updating work items.
        run_row_job: Callback function to process a claimed row.
        claim_input_data: Callback to generate claim request data.
        failure_input_data: Callback to generate failure update data.
        idle_etag: ETag for caching/concurrency control.
    """

    def __init__(
            self,
            get_claimable_url: str,
            put_claim_url: str,
            put_update_baseurl: str,
            run_row_job: Callable[[Worker], None],
            claim_input_data: Optional[Callable[[dict], dict]] = None,
            failure_input_data: Optional[Callable[[dict, Exception], dict]] = None
    ) -> None:
        """Initialize a WorkUnit.

        Args:
            get_claimable_url: URL path to query for claimable work items.
            put_claim_url: URL path to claim a work item.
            put_update_baseurl: Base URL path for updating work items.
            run_row_job: Callback function to process a claimed row.
            claim_input_data: Optional callback to generate claim data.
                Defaults to setting status to "in progress".
            failure_input_data: Optional callback to generate failure data.
                Defaults to setting status to "error".
        """
        self.get_claimable_url = get_claimable_url
        self.put_claim_url = put_claim_url
        self.put_update_baseurl = put_update_baseurl
        self.run_row_job = run_row_job

        # Default claim/failure callbacks
        if claim_input_data is None:
            self.claim_input_data = lambda row: {
                'RID': row['RID'],
                config['image_processing_status']: "in progress"
            }
        else:
            self.claim_input_data = claim_input_data

        if failure_input_data is None:
            self.failure_input_data = lambda row, e: {
                'RID': row['RID'],
                config['image_processing_status']: "error"
            }
        else:
            self.failure_input_data = failure_input_data

        self.idle_etag: Optional[str] = None


# Global list of work units (populated in main())
_work_units: list[WorkUnit] = []


def image_row_job(handler: Worker) -> None:
    """Process an image row job.

    This is a wrapper that delegates to tiff_row_job for processing.

    Args:
        handler: Worker instance containing the row and unit to process.
    """
    tiff_row_job(handler)


def tiff_row_job(handler: Worker) -> None:
    """Generate a tiled pyramid from a source image file.

    Processes the image specified in the handler's row using DerivaImagingWorker.
    Raises WorkerRuntimeError if processing fails.

    Args:
        handler: Worker instance containing the row and unit to process.

    Raises:
        WorkerRuntimeError: If the worker script fails to execute.
    """
    try:
        row = handler.row
        rid = row['RID']
        filename = row.get(config['original_file_name'], 'unknown')

        logger.info('Running job for generating a tiled pyramid for RID="%s" and Filename="%s".', rid, filename)

        deriva_imaging_worker = DerivaImagingWorker(deriva_worker_configuration)
        returncode = deriva_imaging_worker.processImage(rid)

    except Exception:
        et, ev, tb = sys.exc_info()
        logger.error('got unexpected exception "%s"', ev)
        logger.error('%s', ''.join(traceback.format_exception(et, ev, tb)))
        returncode = 1

    if returncode != 0:
        logger.error('Could not execute the worker script')
        raise WorkerRuntimeError('Could not execute the worker script')
    else:
        logger.info('Finished job for generating image for RID="%s" and Filename="%s".', rid, filename)


class Worker:
    """Handler for processing a single work item.

    A Worker instance is created for each claimed row and holds the context
    needed to process it.

    Attributes:
        row: The database row being processed.
        unit: The WorkUnit that defines how to process this row.
        catalog: Class-level PollingErmrestCatalog instance.
        poll_seconds: Class-level polling interval.
        work_units: Class-level list of WorkUnit definitions.
    """

    catalog: PollingErmrestCatalog
    poll_seconds: int
    work_units: list[WorkUnit] = _work_units

    def __init__(self, row: dict[str, Any], unit: WorkUnit) -> None:
        """Initialize a Worker for a claimed row.

        Args:
            row: The database row to process.
            unit: The WorkUnit defining how to process this row.
        """
        logger.info('Claimed job %s.', row.get('RID'))
        self.row = row
        self.unit = unit

    @classmethod
    def look_for_work(cls) -> bool:
        """Find, claim, and process work for each work unit.

        Uses HTTP opportunistic concurrency control and caching for
        efficient polling and quiescence.

        On error, sets Processing_Status to "failed: reason".

        Returns:
            True if work was found and processed, False otherwise.
        """
        found_work = False

        for unit in cls.work_units:
            # Handle concurrent updates safely to claim a record
            try:
                unit.idle_etag, batch = cls.catalog.state_change_once(
                    unit.get_claimable_url,
                    unit.put_claim_url,
                    unit.claim_input_data,
                    unit.idle_etag
                )
            except Exception:
                # Keep going if we have a broken WorkUnit
                continue

            # batch may be empty if no work was found
            for row, claim in batch:
                found_work = True
                try:
                    handler = cls(row, unit)
                    unit.run_row_job(handler)
                except WorkerBadDataError as e:
                    logger.error("Aborting task %s on data error: %s", row["RID"], e)
                    cls.catalog.put(unit.put_claim_url, json=[unit.failure_input_data(row, e)])
                except WorkerRuntimeError as e:
                    logger.error("Aborting task %s on runtime error: %s", row["RID"], e)
                    cls.catalog.put(unit.put_claim_url, json=[unit.failure_input_data(row, e)])
                except Exception as e:
                    cls.catalog.put(unit.put_claim_url, json=[unit.failure_input_data(row, e)])
                    raise

        return found_work

    @classmethod
    def blocking_poll(cls) -> None:
        """Start the blocking poll loop.

        Continuously polls for work using the configured polling interval.
        This method does not return under normal operation.
        """
        cls.catalog.blocking_poll(cls.look_for_work, polling_seconds=cls.poll_seconds)


def main() -> int:
    """CLI entry point for deriva-imaging-server.

    Initializes configuration, sets up the catalog connection, and starts
    the blocking poll loop.

    Returns:
        0 on normal exit, 1 on configuration error.
    """
    global config, deriva_worker_configuration, _work_units

    parser = argparse.ArgumentParser(description='DERIVA imaging pipeline polling server.')
    parser.add_argument('--config', action='store', type=str,
                        help='The JSON configuration file.', required=True)
    args = parser.parse_args()

    # Load configuration
    with open(args.config, 'r') as f:
        config = json.load(f)

    # Initialize logging
    loglevel = config.get('loglevel')
    if loglevel:
        loglevel = _LOGLEVEL.get(loglevel)
    logfile = config.get('log')

    if loglevel and logfile:
        # Clear handlers and reset root logger BEFORE calling init_logging
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.setLevel(logging.NOTSET)
        init_logging(level=loglevel, log_format=FORMAT, file_path=logfile)
    else:
        logging.getLogger().addHandler(logging.NullHandler())

    # Get worker configuration
    deriva_worker_configuration = get_configuration(config, logger)
    if deriva_worker_configuration is None:
        return 1

    # Register work units
    _work_units.append(
        WorkUnit(
            config['get_claimable_url'],
            config['put_claim_url'],
            config['put_update_baseurl'],
            image_row_job
        )
    )

    servername = config['deriva_imaging_server']

    # Load credentials
    credfile = config['credentials_file']
    credentials = json.load(open(credfile))
    if 'cookie' not in credentials.keys():
        credentials = credentials[servername]

    # Create catalog connection (persistent/logical, manages HTTP connection pool)
    catalog = PollingErmrestCatalog(
        'https',
        servername,
        config['catalog_number'],
        credentials
    )
    catalog.dcctx['cid'] = 'pipeline/image/2D/tiff'

    # Configure and start worker
    Worker.poll_seconds = int(os.getenv('DERIVA_IMAGING_POLL_SECONDS', '300'))
    Worker.catalog = catalog
    Worker.blocking_poll()

    return 0


if __name__ == '__main__':
    sys.exit(main())
