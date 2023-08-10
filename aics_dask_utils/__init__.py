# -*- coding: utf-8 -*-

"""Top-level package for AICS Dask Utils."""

from .distributed_handler import DEFAULT_MAX_THREADS, DistributedHandler  # noqa: F401

__author__ = "Jackson Maxfield Brown"
__email__ = "jacksonb@alleninstitute.org"
# Do not edit this string manually, always use bumpversion
# Details in CONTRIBUTING.md
__version__ = "0.2.4"


def get_module_version():
    return __version__
