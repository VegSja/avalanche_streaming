from dataclasses import dataclass


@dataclass
class AvalancheRegion:
    name: str
    region_id: str
    lat: float
    lon: float
