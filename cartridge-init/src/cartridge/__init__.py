"""
Cartridge - AI-powered dbt model generator.

Cartridge scans data sources, analyzes schemas, and uses AI to generate
optimized dbt models with proper documentation and tests.
"""

__version__ = "0.1.0"
__author__ = "Cartridge Team"
__email__ = "team@cartridge.dev"

from cartridge.core.config import settings

__all__ = ["settings", "__version__"]