#!/usr/bin/env python
# -*- coding: utf-8 -*-

import itertools
import json
import logging
import signal
import time
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
# Test function definitions


def spawn_cluster(
    cluster_type: str, cores_per_worker: int, memory_per_worker: str, n_workers: int
) -> Client:
    # Create or get log dir
    log_dir_name = f"c_{cores_per_worker}-mem_{memory_per_worker}-workers_{n_workers}"
    log_dir_time = datetime.now().isoformat().split(".")[0]  # Do not include ms
    log_dir = Path(
        f".dask_logs/{log_dir_time}/{log_dir_name}/{cluster_type}"
    ).expanduser()
    # Log dir settings
    log_dir.mkdir(parents=True, exist_ok=True)

    # Create cluster
    log.info("Creating SLURMCluster")
    cluster = SLURMCluster(
        cores=cores_per_worker,
        memory=memory_per_worker,
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


def run_wait_for_workers_check(client: Client, timeout: int, n_workers: int):
    log.info("Checking wait for workers...")

    # `client.wait_for_workers` is a blocking function, this signal library
    # allows wrapping blocking statements in handlers to check for other stuff
    try:
        # Setup signal check for timeout duration
        signal.signal(signal.SIGALRM, signal_handler)
        signal.alarm(timeout)

        # Actual wait for workers
        client.cluster.scale(n_workers)
        client.wait_for_workers(n_workers)

        # Close signal
        signal.alarm(0)
        log.info("Successfully waited for workers!")
    except TimeoutError:
        log.error("Timed out wait for workers check")

    log.info("Wait for workers check done.")


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


def run_image_read_checks(client: Client, n_workers: int):
    # Spawn workers
    client.cluster.scale(n_workers)

    # Get test image path
    source_image = Path(__file__).parent / "resources" / "example.ome.tiff"

    # Log time duration
    start = time.perf_counter()

    # Run check iterations
    with DistributedHandler(client.cluster.scheduler_address) as handler:
        handler.batched_map(
            run_iteration,
            [source_image for i in range(10000)],
            [source_image.parent / f"{i}.png" for i in range(10000)],
        )

    completion_time = time.perf_counter() - start
    log.info(f"IO checks completed in: {completion_time} seconds")

    return completion_time


def deep_cluster_check(
    cores_per_worker: int,
    memory_per_worker: str,
    n_workers: int,
    timeout: int = 120,  # seconds
):
    completion_time = None
    try:
        log.info(f"Running tests with config: {locals()}")

        log.info("Spawning SLURMCluster...")
        client = spawn_cluster(
            cluster_type="wait_for_workers",
            cores_per_worker=cores_per_worker,
            memory_per_worker=memory_per_worker,
            n_workers=n_workers,
        )

        run_wait_for_workers_check(client=client, timeout=timeout, n_workers=n_workers)
        run_image_read_checks(client=client, n_workers=n_workers)

        log.info("Tearing down cluster.")
        client.shutdown()
        client.close()
        log.info("-" * 80)

        log.info("Waiting a bit for full cluster teardown")
        time.sleep(30)

        log.info("All checks complete")
    except Exception as e:
        log.error(f"An error occurred:")
        log.error(e)

    return completion_time


########################################################################################
# Actual tests


def test_small_workers():
    """
    Run the deep cluster check with small workers.
    Memory per worker is set to 4 * cores per worker.
    Timeout is default to deep cluster check default.

    This is to test the scaling of Dask on SLURM.
    """
    # Run tests
    results = []
    params = itertools.product([1, 2, 4], [12, 64, 128])
    for cores_per_worker, n_workers in params:
        completion_time = deep_cluster_check(
            cores_per_worker=cores_per_worker,
            memory_per_worker=f"{cores_per_worker * 4}GB",
            n_workers=n_workers,
        )
        log.info("=" * 80)

        results.append(
            {
                "cores_per_worker": cores_per_worker,
                "n_workers": n_workers,
                "completion_time": completion_time,
            }
        )

    # Get best config
    best = None
    for config in results:
        if config["completion_time"] is not None:
            if (
                # Handle starting case
                (best is None)
                # Handle new better case
                or (
                    best is not None
                    and config["completion_time"] < best["completion_time"]
                )
            ):
                best = config

    # Log best config
    log.info("=" * 80)
    log.info(f"Cluster config with lowest IO completion_time: {best}")
    log.info("=" * 80)

    # Save results
    with open("aics_cluster_time_results.json", "w") as write_out:
        json.dump(results, write_out)


def test_large_workers():
    """
    Run the deep cluster check with small workers.
    Cores per worker is set to 1 for simplicitly.
    Memory per worker is set 160GB for all tests to lock down a single node.
    N Workers is set to 20, the number of nodes listed as "IDLE" + "MIX" from `sinfo`.
    Timeout is default to deep cluster check default.

    This is to test that all nodes of the cluster are available.
    """
    deep_cluster_check(
        cores_per_worker=1, memory_per_worker="160GB", n_workers=20,
    )
    log.info("=" * 80)


###############################################################################
# Runner


def main():
    test_large_workers()
    log.info("=" * 80)
    log.info("All nodes checked, moving on to cluster config checks.")
    log.info("=" * 80)
    test_small_workers()


###############################################################################
# Allow caller to directly run this module (usually in development scenarios)

if __name__ == "__main__":
    main()
