import math
import logging
from typing import Dict, Any, List, Optional
import requests

logger = logging.getLogger(__name__)

# Base OSRM public routing endpoint
OSRM_BASE_URL = "https://router.project-osrm.org/route/v1/driving"
DEFAULT_TIMEOUT_SECONDS = 8.0

# Urban driving assumptions for fallback estimation
AVERAGE_URBAN_SPEED_KMH = 28.0
URBAN_CIRCUITY_FACTOR = 1.30  # Actual road distance / straight-line distance factor


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate straight-line distance in kilometers between two GPS coordinates using Haversine formula.
    """
    earth_radius_km = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return earth_radius_km * c


def generate_fallback_route(
    start_lat: float,
    start_lon: float,
    end_lat: float,
    end_lon: float,
    error_reason: str,
) -> Dict[str, Any]:
    """
    Generates a sensible straight-line fallback route with urban-adjusted distance and duration
    when the road routing service is unreachable or errors out.
    """
    crow_flies_km = haversine_distance(start_lat, start_lon, end_lat, end_lon)
    estimated_road_km = round(crow_flies_km * URBAN_CIRCUITY_FACTOR, 2)
    # Estimated duration in minutes: (distance / speed) * 60
    estimated_duration_min = round((estimated_road_km / AVERAGE_URBAN_SPEED_KMH) * 60)
    if estimated_duration_min < 1 and estimated_road_km > 0.05:
        estimated_duration_min = 1

    # Folium expects [latitude, longitude]
    points = [
        [round(start_lat, 6), round(start_lon, 6)],
        [round(end_lat, 6), round(end_lon, 6)],
    ]

    # Standard GeoJSON expects [longitude, latitude]
    geojson_feature = {
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            "coordinates": [
                [round(start_lon, 6), round(start_lat, 6)],
                [round(end_lon, 6), round(end_lat, 6)],
            ],
        },
        "properties": {
            "distance_km": estimated_road_km,
            "duration_min": estimated_duration_min,
            "routing_engine": "STRAIGHT_LINE_FALLBACK",
            "is_fallback": True,
            "error_reason": error_reason,
        },
    }

    return {
        "success": False,
        "is_fallback": True,
        "routing_engine": "STRAIGHT_LINE_FALLBACK",
        "distance_km": estimated_road_km,
        "duration_min": estimated_duration_min,
        "points": points,
        "geojson": geojson_feature,
        "error": error_reason,
    }


def get_road_route(
    start_lat: float,
    start_lon: float,
    end_lat: float,
    end_lon: float,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    osrm_url: str = OSRM_BASE_URL,
) -> Dict[str, Any]:
    """
    Fetches a real road-following route from OSRM between start and destination coordinates.

    Note on Coordinate Ordering:
    - OSRM API requires: '{start_lon},{start_lat};{end_lon},{end_lat}'
    - OSRM GeoJSON geometry coordinates are: [longitude, latitude]
    - Folium PolyLine locations expect: [latitude, longitude]

    Returns a standardized dictionary containing:
    - success (bool): Whether OSRM responded successfully
    - is_fallback (bool): True if fallback was used
    - routing_engine (str): 'OSRM' or 'STRAIGHT_LINE_FALLBACK'
    - distance_km (float): Route distance in kilometers
    - duration_min (float): Estimated route duration in minutes
    - points (List[[lat, lon]]): Coordinate list suitable for Folium rendering
    - geojson (dict): Valid GeoJSON LineString Feature (RFC 7946)
    - error (Optional[str]): Error description if routing failed or fallback was triggered
    """
    # 1. Validate coordinates
    try:
        start_lat = float(start_lat)
        start_lon = float(start_lon)
        end_lat = float(end_lat)
        end_lon = float(end_lon)

        if not (-90.0 <= start_lat <= 90.0 and -180.0 <= start_lon <= 180.0):
            return generate_fallback_route(
                start_lat, start_lon, end_lat, end_lon,
                f"Invalid start coordinates: ({start_lat}, {start_lon})"
            )
        if not (-90.0 <= end_lat <= 90.0 and -180.0 <= end_lon <= 180.0):
            return generate_fallback_route(
                start_lat, start_lon, end_lat, end_lon,
                f"Invalid destination coordinates: ({end_lat}, {end_lon})"
            )
    except (TypeError, ValueError) as e:
        return generate_fallback_route(
            0.0, 0.0, 0.0, 0.0, f"Coordinate parsing error: {e}"
        )

    # 2. Check for trivial zero distance
    if abs(start_lat - end_lat) < 1e-6 and abs(start_lon - end_lon) < 1e-6:
        return {
            "success": True,
            "is_fallback": False,
            "routing_engine": "OSRM",
            "distance_km": 0.0,
            "duration_min": 0,
            "points": [[start_lat, start_lon]],
            "geojson": {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[start_lon, start_lat], [start_lon, start_lat]],
                },
                "properties": {
                    "distance_km": 0.0,
                    "duration_min": 0,
                    "routing_engine": "OSRM",
                },
            },
            "error": None,
        }

    # 3. Construct OSRM Request URL with [lon, lat] ordering
    url = f"{osrm_url}/{start_lon},{start_lat};{end_lon},{end_lat}"
    params = {
        "overview": "full",
        "geometries": "geojson",
        "steps": "false",
    }
    headers = {
        "User-Agent": "LastMileAI-MobilityPlatform/1.0",
        "Accept": "application/json",
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        if response.status_code != 200:
            err_msg = f"OSRM HTTP {response.status_code}: {response.text[:120]}"
            logger.warning("Routing service error: %s", err_msg)
            return generate_fallback_route(start_lat, start_lon, end_lat, end_lon, err_msg)

        data = response.json()
        if data.get("code") != "Ok":
            err_msg = f"OSRM API code: {data.get('code')}"
            logger.warning("Routing API response code not Ok: %s", err_msg)
            return generate_fallback_route(start_lat, start_lon, end_lat, end_lon, err_msg)

        routes = data.get("routes", [])
        if not routes:
            err_msg = "OSRM returned no routes between specified coordinates"
            logger.warning(err_msg)
            return generate_fallback_route(start_lat, start_lon, end_lat, end_lon, err_msg)

        primary_route = routes[0]
        coordinates = primary_route.get("geometry", {}).get("coordinates", [])
        if not coordinates:
            err_msg = "OSRM route geometry coordinates are empty"
            return generate_fallback_route(start_lat, start_lon, end_lat, end_lon, err_msg)

        # OSRM coordinates are [lon, lat]. Convert to Folium's [lat, lon]
        folium_points = [[coord[1], coord[0]] for coord in coordinates]

        distance_km = round(primary_route.get("distance", 0.0) / 1000.0, 2)
        duration_min = round(primary_route.get("duration", 0.0) / 60.0, 1)

        geojson_feature = {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": coordinates,  # Valid GeoJSON [lon, lat]
            },
            "properties": {
                "distance_km": distance_km,
                "duration_min": duration_min,
                "routing_engine": "OSRM",
                "is_fallback": False,
            },
        }

        return {
            "success": True,
            "is_fallback": False,
            "routing_engine": "OSRM",
            "distance_km": distance_km,
            "duration_min": duration_min,
            "points": folium_points,
            "geojson": geojson_feature,
            "error": None,
        }

    except requests.Timeout:
        err_msg = f"OSRM request timed out after {timeout}s"
        logger.warning(err_msg)
        return generate_fallback_route(start_lat, start_lon, end_lat, end_lon, err_msg)
    except requests.RequestException as e:
        err_msg = f"OSRM network error: {type(e).__name__} - {str(e)[:80]}"
        logger.warning(err_msg)
        return generate_fallback_route(start_lat, start_lon, end_lat, end_lon, err_msg)
    except Exception as e:
        err_msg = f"Unexpected routing error: {type(e).__name__} - {str(e)[:80]}"
        logger.exception(err_msg)
        return generate_fallback_route(start_lat, start_lon, end_lat, end_lon, err_msg)
