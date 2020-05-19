#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import signal
import time
from datetime import datetime
from pathlib import Path

import dask.config
import pytest
from aicsimageio import AICSImage
from dask_jobqueue import SLURMCluster
from distributed import Client
from imageio import imwrite

from aics_dask_utils import DistributedHandler

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

    # Configure dask config
    dask.config.set({"scheduler.work-stealing": False})

    # Create cluster
    print("Creating SLURMCluster")
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
    print(f"Dask dashboard available at: {cluster.dashboard_link}")

    return client


def signal_handler(signum, frame):
    raise TimeoutError()


def run_wait_for_workers_check(client: Client, timeout: int, n_workers: int):
    # `client.wait_for_workers` is a blocking function, this signal library
    # allows wrapping blocking statements in handlers to check for other stuff
    try:
        print("Starting wait for workers check...")
        # Setup signal check for timeout duration
        signal.signal(signal.SIGALRM, signal_handler)
        signal.alarm(timeout)
        print(f"Will wait for: {timeout} seconds")

        # Actual wait for workers
        client.cluster.scale(n_workers)
        client.wait_for_workers(n_workers)

        # Close signal
        signal.alarm(0)
        print("Successfully waited for workers!")
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


def run_image_read_checks(client: Client, n_workers: int):
    # Spawn workers
    client.cluster.scale(n_workers)

    # Get test image path
    source_image = Path(__file__).parent / "resources" / "example.ome.tiff"

    # Run check iterations
    print("Starting read image iterations...")
    with DistributedHandler(client.cluster.scheduler_address) as handler:
        handler.batched_map(
            run_iteration,
            [source_image for i in range(10000)],
            [source_image.parent / f"{i}.png" for i in range(10000)],
        )


def deep_cluster_check(
    cores_per_worker: int,
    memory_per_worker: str,
    n_workers: int,
    timeout: int = 600,  # seconds
):
    print("Checking wait for workers...")
    print("Spawning SLURMCluster...")
    client = spawn_cluster(
        cluster_type="wait_for_workers",
        cores_per_worker=cores_per_worker,
        memory_per_worker=memory_per_worker,
        n_workers=n_workers,
    )
    run_wait_for_workers_check(client=client, timeout=timeout, n_workers=n_workers)

    print("Wait for workers check done. Tearing down cluster.")
    client.shutdown()
    client.close()
    print("=" * 80)

    print("Waiting a bit for full cluster teardown")
    time.sleep(120)

    print("Checking IO iterations...")
    print("Spawning SLURMCluster...")
    client = spawn_cluster(
        cluster_type="io_iterations",
        cores_per_worker=cores_per_worker,
        memory_per_worker=memory_per_worker,
        n_workers=n_workers,
    )
    # Log time duration
    start = time.perf_counter()
    run_image_read_checks(client=client, n_workers=n_workers)
    print(f"IO checks completed in: {time.perf_counter() - start} seconds")

    print("IO iteration checks done. Tearing down cluster.")
    client.shutdown()
    client.close()
    print("=" * 80)

    print("All checks complete")


########################################################################################
# Actual tests


@pytest.mark.parametrize("cores_per_worker", [1, 2, 4])
@pytest.mark.parametrize("n_workers", [12, 24, 32, 64, 128])
def test_small_workers(cores_per_worker: int, n_workers: int):
    """
    Run the deep cluster check with small workers.
    Memory per worker is set to 4 * cores per worker.
    Timeout is default to deep cluster check default.

    This is to test the scaling of Dask on SLURM.
    """
    deep_cluster_check(
        cores_per_worker=cores_per_worker,
        memory_per_worker=f"{cores_per_worker * 4}GB",
        n_workers=n_workers,
    )


@pytest.mark.parametrize("cores_per_worker", [1, 2, 4, 8, 16])
def test_large_workers(cores_per_worker: int):
    """
    Run the deep cluster check with small workers.
    Memory per worker is set 160GB for all tests to lock down a single node.
    N Workers is set to 22, the number of nodes listed as "IDLE" + "MIX" from `sinfo`.
    Timeout is default to deep cluster check default.

    This is to test that all nodes of the cluster are available.
    """
    deep_cluster_check(
        cores_per_worker=cores_per_worker, memory_per_worker="160GB", n_workers=22,
    )


###############################################################################
# Runner


def main():
    pytest.main()


###############################################################################
# Allow caller to directly run this module (usually in development scenarios)

if __name__ == "__main__":
    main()
