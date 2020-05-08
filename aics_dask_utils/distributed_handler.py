#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from concurrent.futures import Future as ThreadFuture
from concurrent.futures import ThreadPoolExecutor as ThreadClient
from typing import Any, Iterable, List, Optional, Union

from distributed import Client as DaskClient
from distributed import Future as DaskFuture

#######################################################################################

# Equivalent to the default in ThreadPoolExecutor
DEFAULT_MAX_THREADS = os.cpu_count() * 5

#######################################################################################


class DistributedHandler:
    """
    A wrapper around concurrent.futures.ThreadPoolExecutor and distributed.Client to
    make moving from debugging to dask distribution easier and includes additional
    utility functions to manage large iterable mapping.

    Parameters
    ----------
    address: Optional[str]
        A scheduler address to connect to.
        Default: None (Use ThreadPoolExeuctor)

    Examples
    --------
    Use local machine threads for concurrency

    >>> from aics_dask_utils import DistributedHandler
    ... # `None` address provided means use local machine threads
    ... with DistributedHandler(None) as handler:
    ...     futures = handler.client.map(
    ...         lambda x: x + 1,
    ...         [1, 2, 3]
    ...     )
    ...
    ...     results = handler.gather(futures)

    Use some distributed cluster for concurrency

    >>> from distributed import LocalCluster
    ... cluster = LocalCluster()
    ...
    ... # Actual address provided means use the dask scheduler
    ... with DistributedHandler(cluster.scheduler_address) as handler:
    ...     futures = handler.client.map(
    ...         lambda x: x + 1,
    ...         [1, 2, 3]
    ...     )
    ...
    ...     results = handler.gather(futures)
    """

    def __init__(self, address: Optional[str] = None):
        # Create client based off address existance
        if address is None:
            self._client = ThreadClient()
        else:
            self._client = DaskClient(address)

    @property
    def client(self):
        """
        A pointer to the ThreadPoolExecutor or the Distributed Client.
        """
        return self._client

    @staticmethod
    def _get_batch_size(client: Union[DaskClient, ThreadClient]) -> int:
        """
        Returns an integer that matches either the number of Dask workers or number of
        threads available.
        """
        # Handle dask
        if isinstance(client, DaskClient):
            # Using a LocalCluster with processes = False
            if client.cluster is None:
                return DEFAULT_MAX_THREADS

            # In all other cases, there will be a cluster attached
            return len(client.cluster.workers)

        # Return default number of max threads
        return DEFAULT_MAX_THREADS

    def batched_map(
        self, func, *iterables, batch_size: Optional[int] = None, **kwargs,
    ) -> List[Any]:
        """
        Map a function across iterables in a batched fashion.

        If the iterables are of length 1000, but the batch size is 10, it will
        process and _complete_ 10 at a time. Other batch implementations relate
        to how the tasks are submitted to the scheduler itself. See `batch_size`
        parameter on the `distributed.Client.map`:
        https://distributed.dask.org/en/latest/api.html#distributed.Client.map

        This function should be used over `DistributedHandler.client.map` if the map
        operation would result in more than hundreds of thousands of tasks being placed
        on the scheduler.

        See: https://github.com/dask/distributed/issues/2181 for more details

        Parameters
        ----------
        func: Callable
            A serializable callable function to run across each iterable set.
        iterables: Iterables
            List-like objects to map over. They should have the same length.
        batch_size: Optional[int]
            Number of items to process and _complete_ in a single batch.
            Default: number of available workers or threads.
        **kwargs: dict
            Other keyword arguments to pass down to this handler's client.

        Returns
        -------
        results: Iterable[Any]
            The complete results of all items after they have been fully processed
            and gathered.
        """
        # If no batch size was provided, get batch size based off client
        if batch_size is None:
            batch_size = self._get_batch_size(self.client)

        # Batch process iterables
        results = []
        for i in range(0, len(iterables[0]), batch_size):
            this_batch_iterables = []
            for iterable in iterables:
                this_batch_iterables.append(iterable[i : i + batch_size])

            futures = self.client.map(func, *this_batch_iterables, **kwargs,)

            results += self.gather(futures)

        return results

    def gather(self, futures: Iterable[Union[ThreadFuture, DaskFuture]]) -> List[Any]:
        """
        Block until all futures are complete and return in a list of results.

        Parameters
        ----------
        futures: Iterable[Union[ThreadFuture, DaskFuture]]
            An iterable of futures object returned from DistributedHandler.client.map.

        Returns
        -------
        results: List[Any]
            The result of each future in a list.
        """
        if isinstance(self.client, ThreadClient):
            return list(futures)
        else:
            return self.client.gather(futures)

    def close(self):
        """
        Close whichever client this handler is holding open.

        Note: If connected to a Distributed.Client, it will close the client connection
        but will not shutdown the cluster.
        """
        if isinstance(self.client, ThreadClient):
            self.client.shutdown()
        else:
            self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
