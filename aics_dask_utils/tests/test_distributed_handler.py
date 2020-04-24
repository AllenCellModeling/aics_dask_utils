#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from aics_dask_utils import DistributedHandler

from concurrent.futures import ThreadPoolExecutor
from distributed import Client, LocalCluster


@pytest.mark.parametrize(
    "values, expected_values",
    [
        (range(10), range(1, 11)),
        (range(100), range(1, 101)),
        (range(1000), range(1, 1001)),
    ],
)
def test_distributed_handler_threadpool(values, expected_values):
    with DistributedHandler() as handler:
        futures = handler.client.map(lambda x: x + 1, values,)
        handler_map_results = handler.gather(futures)

    with DistributedHandler() as handler:
        handler_batched_results = handler.batched_map(lambda x: x + 1, values)

    with ThreadPoolExecutor() as exe:
        futures = exe.map(lambda x: x + 1, values)
        threadpool_results = list(futures)

    handler_map_results = set(handler_map_results)
    handler_batched_results = set(handler_batched_results)
    threadpool_results = set(threadpool_results)

    assert (
        handler_map_results == handler_batched_results
        and handler_map_results == threadpool_results
    )


@pytest.mark.parametrize(
    "values, expected_values",
    [
        (range(10), range(1, 11)),
        (range(100), range(1, 101)),
        (range(1000), range(1, 1001)),
    ],
)
def test_distributed_handler_distributed(values, expected_values):
    cluster = LocalCluster(processes=False)

    with DistributedHandler(cluster.scheduler_address) as handler:
        futures = handler.client.map(lambda x: x + 1, values)
        handler_map_results = handler.gather(futures)

    with DistributedHandler(cluster.scheduler_address) as handler:
        handler_batched_results = handler.batched_map(lambda x: x + 1, values)

    client = Client(cluster)
    futures = client.map(lambda x: x + 1, values)

    distributed_results = client.gather(futures)

    handler_map_results = set(handler_map_results)
    handler_batched_results = set(handler_batched_results)
    distributed_results = set(distributed_results)

    assert (
        handler_map_results == handler_batched_results
        and handler_map_results == distributed_results
    )
