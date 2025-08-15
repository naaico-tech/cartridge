"""
Cartridge-Warp: CDC Streaming Platform

A modular Change Data Capture (CDC) streaming platform for real-time
and batch data synchronization between various databases.
"""

__version__ = "0.1.0"
__author__ = "Cartridge Team"
__email__ = "team@cartridge.dev"

from .core.config import WarpConfig
from .core.runner import WarpRunner

__all__ = ["WarpConfig", "WarpRunner"]
