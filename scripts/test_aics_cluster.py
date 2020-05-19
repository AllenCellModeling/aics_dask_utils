#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import json
import logging
import signal
import time
import traceback
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
    cluster_type: str,
    cores_per_worker: int,
    memory_per_worker: str,
    n_workers: int
) -> Client:
    # Create or get log dir
    log_dir_name = f"c_{cores}-mem_{memory}-workers_{n_workers}"
    log_dir_time = datetime.now().isoformat().split(".")[0]  # Do not include ms
    log_dir = Path(
        f".dask_logs/{log_dir_time}/{log_dir_name}/{cluster_type}"
    ).expanduser()
    # Log dir settings
    log_dir.mkdir(parents=True, exist_ok=True)

    # Configure dask config
    dask.config.set(
        {"scheduler.work-stealing": False}
    )

    # Create cluster
    log.info("Creating SLURMCluster")
    cluster = SLURMCluster(
        cores=cores,
        memory=memory,
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
    # `client.wait_for_workers` is a blocking function, this signal library
    # allows wrapping blocking statements in handlers to check for other stuff
    try:
        log.info("Starting wait for workers check...")
        # Setup signal check for timeout duration
        signal.signal(signal.SIGALRM, signal_handler)
        signal.alarm(timeout)
        log.info(f"Will wait for: {timeout} seconds")

        # Actual wait for workers
        client.cluster.scale(n_workers)
        client.wait_for_workers(n_workers)

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


def run_image_read_checks(client: Client, n_workers: int):
    # Spawn workers
    client.cluster.scale(n_workers)

    # Get test image path
    source_image = Path(__file__).parent / "resources" / "example.ome.tiff"

    # Run check iterations
    log.info("Starting read image iterations...")
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
    timeout: int = 600  # seconds
):
    log.info("Checking wait for workers...")
    log.info("Spawning SLURMCluster...")
    client = spawn_cluster(
        cluster_type="wait_for_workers",
        cores_per_worker=cores_per_worker,
        memory_per_worker=memory_per_worker,
        n_workers=n_workers,
    )
    run_wait_for_workers_check(client=client, timeout=timeout, n_workers=n_workers)

    log.info("Wait for workers check done. Tearing down cluster.")
    client.shutdown()
    client.close()
    log.info("=" * 80)

    log.info("Waiting a bit for full cluster teardown")
    time.sleep(120)

    log.info("Checking IO iterations...")
    log.info("Spawning SLURMCluster...")
    client = spawn_cluster(
        cluster_type="io_iterations",
        cores_per_worker=cores_per_worker,
        memory_per_worker=memory_per_worker,
        n_workers=n_workers,
    )
    # Log time duration
    start = time.perf_counter()
    run_image_read_checks(client=client, n_workers=n_workers)
    log.info(f"IO checks completed in: {time.perf_counter() - start} seconds")

    log.info("IO iteration checks done. Tearing down cluster.")
    client.shutdown()
    client.close()
    log.info("=" * 80)

    log.info("All checks complete")

########################################################################################
# Actual tests


@pytest.mark.parametrize("cores_per_worker", [1, 2, 4])
@pytest.mark.parametrize("n_workers", [12, 24, 32, 64, 128])
def test_small_workers(caplog, cores_per_worker: int, n_workers: int):
    """
    Run the deep cluster check with small workers.
    Memory per worker is set to 4 * cores per worker.
    Timeout is default to deep cluster check default.

    This is to test the scaling of Dask on SLURM.
    """
    caplog.set_level(logging.INFO)
    deep_cluster_check(
        cores_per_worker=cores_per_worker,
        memory_per_worker=[f"{cpw * 4}GB" for cpw in cores_per_worker],
        n_workers=n_workers,
    )


@pytest.mark.parametrize("cores_per_worker", [1, 2, 4, 8, 16])
def test_large_workers(caplog, cores_per_worker: int, n_workers: int):
    """
    Run the deep cluster check with small workers.
    Memory per worker is set 160GB for all tests to lock down a single node.
    N Workers is set to 22, the number of nodes listed as "IDLE" + "MIX" from `sinfo`.
    Timeout is default to deep cluster check default.

    This is to test that all nodes of the cluster are available.
    """
    caplog.set_level(logging.INFO)
    deep_cluster_check(
        cores_per_worker=cores_per_worker,
        memory_per_worker="160GB",
        n_workers=22,
    )