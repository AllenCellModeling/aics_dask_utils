# AICS Dask Utils

[![Build Status](https://github.com/AllenCellModeling/aics_dask_utils/workflows/Build%20Master/badge.svg)](https://github.com/AllenCellModeling/aics_dask_utils/actions)
[![Documentation](https://github.com/AllenCellModeling/aics_dask_utils/workflows/Documentation/badge.svg)](https://AllenCellModeling.github.io/aics_dask_utils)
[![Code Coverage](https://codecov.io/gh/AllenCellModeling/aics_dask_utils/branch/master/graph/badge.svg)](https://codecov.io/gh/AllenCellModeling/aics_dask_utils)

Documentation related to Dask, Distributed, and related packages.
Utility functions commonly used by AICS projects.

---

## Features
* Distributed handler to manage various debugging or cluster configurations
* Documentation on example cluster deployments

## Basics
Before we jump into quick starts there are some basic definitions to understand.

#### Task
A task is a single static function to be processed. Simple enough. However, relevant to
AICS, is that when using `aicsimageio` (and / or `dask.array.Array`), your image (or
`dask.array.Array`) is split up into _many_ tasks. This is dependent on the image reader
and the size of the file you are reading. But in general it is safe to assume that each
image you read is split many thousands of tasks. If you want to see how many tasks your
image is split into you can either compute:

1. Psuedo-code: `sum(2 * size(channel) for channel if channel not in ["Y", "X"])`
2. Dask graph length: `len(AICSImage.dask_data.__dask_graph__())`

#### Map
Apply a given function to the provided iterables as used as parameters to the function.
Given `lambda x: x + 1` and `[1, 2, 3]`, the result of `map(func, *iterables)` in this
case would be `[2, 3, 4]`. Usually, you are provided back an iterable of `future`
objects back from a `map` operation. The results from the map operation are not
guaranteed to be in the order of the iterable that went in as operations are started as
resources become available and item to item variance may result in different output
ordering.

#### Future
An object that will become available but is currently not defined. There is no guarantee
that the object is a valid result or an error and you should handle errors once the
future's state has resolved (usually this means after a `gather` operation).

#### Gather
Block the process from moving forward until all futures are resolved. Control flow here
would mean that you could potentially generate thousands of futures and keep moving on
locally while those futures slowly resolve but if you ever want a hard stop and wait for
some set of futures to complete, you would need gather them.

##### Other Comments
Dask tries to mirror the standard library `concurrent.futures` wherever possible which
is what allows for this library to have simple wrappers around Dask to allow for easy
debugging as we are simply swapping out `distributed.Client.map` with
`concurrent.futures.ThreadPoolExecutor.map` for example. If at any point in your code
you don't want to use `dask` for some reason or another, it is equally valid to use
`concurrent.futures.ThreadPoolExecutor` or `concurrent.futures.ProcessPoolExecutor`.

### Basic Mapping with Distributed Handler
If you have an iterable (or iterables) that would result in less than hundreds of
thousands of tasks, it you can simply use the normal `map` provided by the
`DistributedHandler.client`.

**Important Note:** Notice, "... iterable that would _result_ in less than hundreds
of thousands of tasks...". This is important because what happens when you try to `map`
over a thousand image paths, each which spawns an `AICSImage` object. Each one adds
thousands more tasks to the scheduler to complete. This will break and you should look
to [Large Iterable Batching](#large-iterable-batching) instead.

```python
from aics_dask_utils import DistributedHandler

# `None` address provided means use local machine threads
with DistributedHandler(None) as handler:
    futures = handler.client.map(
        lambda x: x + 1,
        [1, 2, 3]
    )

    results = handler.gather(futures)

from distributed import LocalCluster
cluster = LocalCluster()

# Actual address provided means use the dask scheduler
with DistributedHandler(cluster.scheduler_address) as handler:
    futures = handler.client.map(
        lambda x: x + 1,
        [1, 2, 3]
    )

    results = handler.gather(futures)
```

### Large Iterable Batching
If you have an iterable (or iterables) that would result in more than hundreds of
thousands of tasks, you should use `handler.batched_map` to reduce the load on the
client. This will batch your requests rather than send than all at once.

```python
from aics_dask_utils import DistributedHandler

# `None` address provided means use local machine threads
with DistributedHandler(None) as handler:
    results = handler.batched_map(
        lambda x: x + 1,
        range(1e9) # 1 billion
    )

from distributed import LocalCluster
cluster = LocalCluster()

# Actual address provided means use the dask scheduler
with DistributedHandler(cluster.scheduler_address) as handler:
    results = handler.batched_map(
        lambda x: x + 1,
        range(1e9) # 1 billion
    )
```

**Note:** Notice that there is no `handler.gather` call after `batched_map`. This is
because `batched_map` gathers results at the end of each batch rather than simply
returning their future's.

## Installation
**Stable Release:** `pip install aics_dask_utils`<br>
**Development Head:** `pip install git+https://github.com/AllenCellModeling/aics_dask_utils.git`

## Documentation
For full package documentation please visit
[AllenCellModeling.github.io/aics_dask_utils](https://AllenCellModeling.github.io/aics_dask_utils).

## Development
See [CONTRIBUTING.md](CONTRIBUTING.md) for information related to developing the code.

## Additional Comments
This README, provided tooling, and documentation are not meant to be all encompassing
of the various operations you can do with `dask` and other similar computing systems.
For further reading go to [dask.org](https://dask.org/).

**Free software: Allen Institute Software License**
