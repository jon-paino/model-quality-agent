"""Geohash and Haversine helpers. Deterministic, vectorized where it matters."""

from __future__ import annotations

import math

import geohash  # python-geohash
import numpy as np
import pandas as pd

from .config import GEOHASH_PRECISION


def encode_geohash(lat: float, lon: float, precision: int = GEOHASH_PRECISION) -> str:
    """Encode a single (lat, lon) to a geohash string at the given precision."""
    return geohash.encode(lat, lon, precision=precision)


def encode_geohash_series(
    lat: pd.Series, lon: pd.Series, precision: int = GEOHASH_PRECISION
) -> pd.Series:
    """Vectorized geohash encoding over pandas Series."""
    return pd.Series(
        [geohash.encode(la, lo, precision=precision) for la, lo in zip(lat, lon)],
        index=lat.index,
        dtype="string",
    )


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points in miles."""
    r_miles = 3958.7613
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2.0) ** 2
    return 2.0 * r_miles * math.asin(math.sqrt(a))


def haversine_miles_vec(
    lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray, lon2: np.ndarray
) -> np.ndarray:
    """Vectorized Haversine."""
    r_miles = 3958.7613
    p1 = np.radians(lat1)
    p2 = np.radians(lat2)
    dp = np.radians(lat2 - lat1)
    dl = np.radians(lon2 - lon1)
    a = np.sin(dp / 2.0) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2.0) ** 2
    return 2.0 * r_miles * np.arcsin(np.sqrt(a))
