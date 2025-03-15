from dataclasses import dataclass


@dataclass
class AvalancheRegion:
    name: str
    region_id: str
    west_north_lat: float
    west_north_lon: float
    east_south_lat: float
    east_south_lon: float
