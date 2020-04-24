# Distributed Cluster Deployments

These are all _tested and working_ examples.

## LocalCluster
If you want to turn your local machine into a `distributed.Cluster` simply run:
```python
from distributed import Client, LocalCluster

cluster = LocalCluster()
client = Client(cluster)
```

## AICS SLURM Cluster
```python
from datetime import datetime

import dask.config
from dask_jobqueue import SLURMCluster
from distributed import Client

# Create or get log dir
# Do not include ms
log_dir_name = datetime.now().isoformat().split(".")[0]
log_dir = Path(f".dask_logs/{log_dir_name}").expanduser()
# Log dir settings
log_dir.mkdir(parents=True, exist_ok=True)

# Configure dask config
dask.config.set(
    {
        "scheduler.work-stealing": False,
    }
)

# Create cluster
cluster = SLURMCluster(
    cores=12,
    memory="160GB",
    queue="aics_cpu_general",
    walltime="10:00:00",
    local_directory=str(log_dir),
    log_directory=str(log_dir),
)

# Scale cluster
cluster.scale(12)

# Create client connection
client = Client(cluster)
```

## AWS Fargate Cluster
Requires all data needed be accessible to workers. In this case, you should probably put
the data on S3 and use [s3fs](https://github.com/dask/s3fs). You must also upload a
Docker image to [Docker Hub](https://hub.docker.com/).

```python
from dask_cloudprovider import FargateCluster
from distributed import Client

# Create connection
cluster = FargateCluster("username/dockerimage")

# Adapt
cluster.adapt(minimum_jobs=1, maximum_jobs=100)

# Create client connection
client = Client(cluster)
```


#### Example Dockerfile
```
FROM ubuntu:18.04

# Copy project
COPY . /project

# General upgrades and requirements
RUN apt-get update && apt-get upgrade -y

# Get software props
RUN apt-get install -y \
    software-properties-common

# Add additional apt repository
RUN add-apt-repository universe

# Get python3.7 and pip
RUN apt-get update && apt-get install -y \
    python3.7 \
    python3.7-dev \
    python3-pip \
    git

# Upgrade pip and force it to use python3.7
RUN python3.7 -m pip install --upgrade pip

# Set python3.7 to default python
RUN ln -sf /usr/bin/python3.7 /usr/bin/python
RUN ln -sf /usr/bin/python3.7 /usr/bin/python3

# Set workdir
WORKDIR project/

# Install package
RUN pip install .
```
