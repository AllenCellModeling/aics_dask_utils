#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import signal
import time
import traceback
from datetime import datetime
from pathlib import Path

import dask.config
from aicsimageio import AICSImage
from dask_jobqueue import SLURMCluster
from distributed import Client
from imageio import imwrite

from aics_dask_utils import DistributedHandler

###############################################################################

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)4s: %(module)s:%(lineno)4s %(asctime)s] %(message)s",
)
log = logging.getLogger(__name__)

###############################################################################
# Args


class Args(argparse.Namespace):
    def __init__(self):
        self.__parse()

    def __parse(self):
        # Setup parser
        p = argparse.ArgumentParser(
            prog="check_aics_cluster",
            description=(
                "Run a deep check on the AICS SLURM cluster to ensure that all nodes "
                "are accessible."
            ),
        )

        # Arguments
        p.add_argument(
            "-i",
            "--iterations",
            type=int,
            default=500,
            dest="iterations",
            help=("Number of times the file should be loaded and saved."),
        )
        p.add_argument(
            "-t",
            "--wait-for-workers-timeout",
            type=int,
            default=600,
            dest="timeout",
            help=(
                "Number of seconds to wait before cancelling a wait_for_workers call."
            ),
        )
        p.add_argument(
            "-n",
            "--n-workers",
            type=int,
            default=22,
            dest="n_workers",
            help=("Number of dask workers to create."),
        )

        # Parse
        p.parse_args(namespace=self)


###############################################################################
# Check function definitions


def spawn_cluster(args: Args, cluster_type: str) -> Client:
    # Create or get log dir
    # Do not include ms
    log_dir_name = datetime.now().isoformat().split(".")[0]
    log_dir = Path(f".dask_logs/{cluster_type}/{log_dir_name}").expanduser()
    # Log dir settings
    log_dir.mkdir(parents=True, exist_ok=True)

    # Configure dask config
    dask.config.set(
        {"scheduler.work-stealing": False}
    )

    # Create cluster
    log.info("Creating SLURMCluster")
    cluster = SLURMCluster(
        cores=4,
        memory="160GB",  # One worker per node
        queue="aics_cpu_general",
        walltime="10:00:00",
        local_directory=str(log_dir),
        log_directory=str(log_dir),
    )

    # Create client connection
    client = Client(cluster)
    log.info(f"Dask dashboard available at: {cluster.dashboard_link}")

    return client


def signal_handler(signum, frame):
    raise TimeoutError()


def run_wait_for_workers_check(args: Args, client: Client):
    # `client.wait_for_workers` is a blocking function, this signal library
    # allows wrapping blocking statements in handlers to check for other stuff
    try:
        log.info("Starting wait for workers check...")
        # Setup signal check for timeout duration
        signal.signal(signal.SIGALRM, signal_handler)
        signal.alarm(args.timeout)
        log.info(f"Will wait for: {args.timeout} seconds")

        # Actual wait for workers
        client.cluster.scale(args.n_workers)
        client.wait_for_workers(args.n_workers)

        # Close signal
        signal.alarm(0)
        log.info("Successfully waited for workers!")
    except TimeoutError:
        log.error("Timed out wait for workers check")


def run_iteration(file: Path, save_path: Path) -> Path:
    # Read image
    img = AICSImage(file)

    # Select middle slice of structure channel
    data = img.get_image_data(
        "YX", S=0, T=0, C=img.get_channel_names().index("structure"), Z=img.size_z // 2,
    )

    # Write out image as png
    imwrite(save_path, data)

    return save_path


def run_image_read_checks(args: Args, client: Client):
    # Spawn workers
    client.cluster.scale(args.n_workers)

    # Get test image path
    source_image = Path(__file__).parent / "resources" / "example.ome.tiff"

    # Run check iterations
    log.info("Starting read image iterations...")
    with DistributedHandler(client.cluster.scheduler_address) as handler:
        handler.batched_map(
            run_iteration,
            [source_image for i in range(args.iterations)],
            [source_image.parent / f"{i}.png" for i in range(args.iterations)],
        )


def run_all_checks(args: Args):
    # Try running the entire check
    try:
        log.info("Checking wait for workers...")
        log.info("Spawning SLURMCluster...")
        client = spawn_cluster(args, "wait_for_workers")
        run_wait_for_workers_check(args, client)

        log.info("Wait for workers check done. Tearing down cluster.")
        client.shutdown()
        client.close()
        log.info("=" * 80)

        log.info("Waiting a bit for full cluster teardown")
        time.sleep(120)

        log.info("Checking IO iterations...")
        log.info("Spawning SLURMCluster...")
        client = spawn_cluster(args, "io_iterations")
        run_image_read_checks(args, client)

        log.info("IO iteration checks done. Tearing down cluster.")
        client.shutdown()
        client.close()
        log.info("=" * 80)

        log.info("All checks complete")

    # Catch any exception
    except Exception as e:
        log.error("=============================================")
        log.error("\n\n" + traceback.format_exc())
        log.error("=============================================")
        log.error("\n\n" + str(e) + "\n")
        log.error("=============================================")


###############################################################################
# Runner


def main():
    args = Args()
    run_all_checks(args)


###############################################################################
# Allow caller to directly run this module (usually in development scenarios)

if __name__ == "__main__":
    main()
