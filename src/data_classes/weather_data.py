from dataclasses import dataclass

from data_classes.daily_data import DailyData, DailyUnits


@dataclass
class WeatherData:
    latitude: float
    longitude: float
    generationtime_ms: float
    utc_offset_seconds: float
    timezone: str
    timezone_abbreviation: str
    elevation: float
    daily: "DailyData"
    daily_units: "DailyUnits"
