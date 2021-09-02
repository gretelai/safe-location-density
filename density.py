"""
Utilities for processing location data into privacy
preserving location densities
"""
import re
from functools import wraps
import uuid
from dataclasses import dataclass

import pandas as pd
import h3pandas  # noqa
import folium
from geopandas import GeoDataFrame
from folium.features import Choropleth


AGG = "agg"
EXTRAP = "extrapolate"
GEO = "geometry"
LAT = "lat"
LNG = "lng"
MODES = [AGG, EXTRAP]
H3_RE = re.compile(r"^h3_\d{2}$")


def _poly_cache_key(lat, lng) -> str:
    return f"{lat}-{lng}"


def _needs_fit(func):
    @wraps(func)
    def _wrapper(self: "DensityTransform", *args, **kwargs):
        if not self._is_fit:
            raise ValueError("This method requires fit() to be called first")

        mode = kwargs.get("mode", None)
        if mode and mode not in MODES:
            raise ValueError(f"Invalid mode, must be one of: {MODES}")

        return func(self, *args, **kwargs)

    return _wrapper


_resolution_values = [
    (4_250_546.8477000, 1_107.712591000),
    (607_220.9782429, 418.676005500),
    (86_745.8540347, 158.244655800),
    (12_392.2648621, 59.810857940),
    (1_770.3235517, 22.606379400),
    (252.9033645, 8.544408276),
    (36.1290521, 3.229482772),
    (5.1612932, 1.220629759),
    (0.7373276, 0.461354684),
    (0.1053325, 0.174375668),
    (0.0150475, 0.065907807),
    (0.0021496, 0.024910561),
    (0.0003071, 0.009415526),
    (0.0000439, 0.003559893),
    (0.0000063, 0.001348575),
    (0.0000009, 0.000509713),
]
"""Index of a tuple maps to the resolution number"""


@dataclass
class H3Resolution:
    """
    Dataclass representation of https://h3geo.org/docs/core-library/restable/
    """

    resolution: int
    """The resolution number ranging from 0..15"""

    area_km2: float
    """The amount of square kilometers covered by the hex polygon"""

    avg_edge_len_km: float
    """Average length of a hex edge in kilometers"""

    @classmethod
    def from_resolution(cls, res: int) -> "H3Resolution":
        area, edge_len = _resolution_values[res]
        return cls(resolution=res, area_km2=area, avg_edge_len_km=edge_len)


class DensityTransform:

    _input_df: pd.DataFrame
    _id_col: str
    _lat_col: str = None
    _lng_col: str = None
    _polygon_cache: dict
    _is_fit: bool = False
    _resolution: H3Resolution

    def __init__(
        self,
        *,
        df: pd.DataFrame,
        id_col: str,
        lat_col: str = LAT,
        lng_col: str = LNG,
    ):
        self._input_df = df.copy()
        self._id_col = id_col
        self._lat_col = lat_col
        self._lng_col = lng_col
        self._polygon_cache = {}

    def fit(self, *, resolution: int) -> "DensityTransform":
        self._input_df = (
            self._input_df.h3.geo_to_h3(
                resolution, lat_col=self._lat_col, lng_col=self._lng_col
            )
            .h3.h3_to_geo_boundary()
            .reset_index()
        )

        # We're not really using this column so we'll try to remove it
        _hex_columns = [
            col for col in self._input_df.columns if H3_RE.match(col)
        ]  # noqa
        if len(_hex_columns) == 1:
            self._input_df.drop(_hex_columns, axis=1, inplace=True)

        # Do an inplace replacement of our coordinates with the centroids
        # of the hex grids at the specified resolution
        self._input_df[self._lng_col] = self._input_df[GEO].apply(
            lambda obj: obj.centroid.coords.xy[0][0]
        )
        self._input_df[self._lat_col] = self._input_df[GEO].apply(
            lambda obj: obj.centroid.coords.xy[1][0]
        )

        # build the polygon cache to map lat/long to the Polygon objects as
        # we cannot aggregate by these objects
        for _, row in self._input_df.iterrows():
            cache_key = _poly_cache_key(row[self._lat_col], row[self._lng_col])
            self._polygon_cache[cache_key] = row[GEO]

        self._is_fit = True
        self.resolution = H3Resolution.from_resolution(resolution)

        return self

    @_needs_fit
    def transform(
        self, *, mode: str = AGG
    ) -> pd.DataFrame:
        if mode == AGG:
            return self._transform_agg()

    @_needs_fit
    def transform_plot(self, *, mode: str = AGG) -> folium.Map:
        if mode == AGG:
            agg_df = self._transform_agg(restore_geo=True)
            geo_df = GeoDataFrame(agg_df)
            geo_df.set_crs(epsg=4326, inplace=True)
            random_col = uuid.uuid4().hex
            geo_df[random_col] = geo_df.index
            center = geo_df[[self._lat_col, self._lng_col]].mean()
            _map = folium.Map(location=list(center))
            c = Choropleth(
                geo_data=geo_df,
                data=geo_df,
                columns=[random_col, self._id_col],
                key_on=f"feature.properties.{random_col}",
                legend_name=f"Unique count of: {self._id_col}",
            )
            c.add_to(_map)
            return _map

    def _transform_agg(self, restore_geo: bool = False) -> pd.DataFrame:
        _group_by = [self._lat_col, self._lng_col]
        agg_df = (
            self._input_df.groupby(_group_by)
            .agg({self._id_col: "nunique"})
            .reset_index()
        )

        # restore the Polygon objects if needed
        if restore_geo:
            agg_df[GEO] = agg_df.apply(
                lambda row: self._polygon_cache.get(
                    _poly_cache_key(row[self._lat_col], row[self._lng_col])
                ),
                axis=1,
            )

        return agg_df
