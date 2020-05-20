# Scripts

## check_aics_cluster.py

A script to run deep checks on the AICS SLURM cluster and its interactions with Dask.
It checks:
* All SLURM nodes (or at least many SLURM nodes)
* `distributed.Client.wait_for_workers` to ensure that all nodes start up as workers
* IO checks with `aicsimageio>=3.2.0` to ensure that Dask IO isn't failing on any worker
* Timings for completing the 10000 IO iterations against the various cluster configs

### Full Commands to Run (start on SLURM master)
```bash
srun -c 8 --mem 40GB -p aics_cpu_general --pty bash
git clone git@github.com:AllenCellModeling/aics_dask_utils.git
cd aics_dask_utils
conda create --name aics_dask_utils python=3.7 -y
conda activate aics_dask_utils
pip install -e .[deepcheck]
python scripts/check_aics_cluster.py
```

Dask worker logs from each check will be placed in:
`.dask_logs/{cluster_creation_time}/{config}/{test_type}`
