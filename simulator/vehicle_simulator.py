import os
import sys
from pathlib import Path

# Ensure project root is in sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import time
import random
import json
import math
import requests
from datetime import datetime, timezone

from services.streaming.producer import producer as kafka_producer
from services.streaming.config import config as streaming_config

# Backend API endpoints (resolves from environment or dynamic cloud PORT)
PORT = os.getenv("PORT", "8000")
BASE_URL = os.getenv("BASE_URL") or os.getenv("API_URL") or f"http://127.0.0.1:{PORT}"
BASE_URL = BASE_URL.rstrip("/")
EVENTS_URL = f"{BASE_URL}/events"
ORDERS_URL = f"{BASE_URL}/orders/vehicle"

# Initial vehicle positions (Delhi NCR)
vehicles = {
    "V101": {"lat": 28.6139, "lon": 77.2090, "dest_lat": 28.7300, "dest_lon": 77.2900, "order_id": "ORD-2001"},
    "V102": {"lat": 28.6200, "lon": 77.2150, "dest_lat": 28.7000, "dest_lon": 77.3200, "order_id": "ORD-2002"},
    "V103": {"lat": 28.6300, "lon": 77.2200, "dest_lat": 28.6900, "dest_lon": 77.3500, "order_id": "ORD-2003"},
    "V104": {"lat": 28.6400, "lon": 77.2300, "dest_lat": 28.7400, "dest_lon": 77.3800, "order_id": "ORD-2004"},
    "V105": {"lat": 28.6500, "lon": 77.2400, "dest_lat": 28.7100, "dest_lon": 77.3650, "order_id": "ORD-2005"}
}

def sync_vehicle_order(vehicle_id: str):
    """Attempt to sync current assigned destination from backend order engine."""
    try:
        res = requests.get(f"{ORDERS_URL}/{vehicle_id}", timeout=2)
        if res.status_code == 200:
            orders = res.json()
            active = next((o for o in orders if o.get("order_status") != "DELIVERED"), None)
            if active:
                dest_lat = active.get("destination_latitude") or active.get("destination", {}).get("lat")
                dest_lon = active.get("destination_longitude") or active.get("destination", {}).get("lon")
                if dest_lat and dest_lon:
                    vehicles[vehicle_id]["dest_lat"] = float(dest_lat)
                    vehicles[vehicle_id]["dest_lon"] = float(dest_lon)
                vehicles[vehicle_id]["order_id"] = active.get("order_id", vehicles[vehicle_id]["order_id"])
    except requests.RequestException:
        pass  # Graceful fallback to initial coordinates if backend not ready

def run_simulator_loop(stop_event=None):
    """Continuous simulation worker loop."""
    print("====================================================")
    print("LASTMILE AI - REAL-TIME VEHICLE & ORDER SIMULATOR")
    print("====================================================")
    print("Simulating 5 fleet vehicles with dynamic order destinations...")
    time.sleep(2)  # Short delay for server readiness

    start_time = time.time()
    iteration = 0

    while True:
        if stop_event and stop_event.is_set():
            break

        iteration += 1

        for vehicle_id, vehicle in vehicles.items():
            # Every 6 ticks (~30s), check backend for order reassignment/updates
            if iteration % 6 == 1:
                sync_vehicle_order(vehicle_id)

            # Simulate traffic disruption for V102 (stopped for first 40s to trigger stagnation alerts)
            elapsed = time.time() - start_time
            if vehicle_id == "V102" and elapsed < 40:
                speed = 0.0
            else:
                speed = float(random.choice([15, 20, 25, 30, 35, 40]))

            # Heading calculation based on directional movement vector
            delta_lat = vehicle["dest_lat"] - vehicle["lat"]
            delta_lon = vehicle["dest_lon"] - vehicle["dest_lon"]
            dist_to_dest = math.hypot(delta_lat, delta_lon)
            heading = (math.degrees(math.atan2(delta_lon, delta_lat)) + 360) % 360 if speed > 2 else 0.0

            # Simulate directional movement towards assigned delivery destination
            if speed > 0:
                if dist_to_dest > 0.001:
                    step_size = (speed / 3600.0) * (5.0 / 111.0)
                    ratio = min(step_size / dist_to_dest, 1.0)
                    vehicle["lat"] += delta_lat * ratio + random.uniform(-0.00005, 0.00005)
                    vehicle["lon"] += delta_lon * ratio + random.uniform(-0.00005, 0.00005)
                else:
                    speed = 2.0
                    sync_vehicle_order(vehicle_id)

            v_status = "STOPPED" if speed <= 2 else "MOVING"

            # 1. Publish directly via Kafka Producer
            try:
                kafka_producer.publish_vehicle_telemetry(
                    vehicle_id=vehicle_id,
                    latitude=round(vehicle["lat"], 6),
                    longitude=round(vehicle["lon"], 6),
                    speed=round(speed, 1),
                    heading=round(heading, 1),
                    vehicle_status=v_status
                )
            except Exception:
                pass

            # 2. REST telemetry update
            event = {
                "vehicle_id": vehicle_id,
                "latitude": round(vehicle["lat"], 6),
                "longitude": round(vehicle["lon"], 6),
                "speed": round(speed, 1),
                "heading": round(heading, 1),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event_type": "GPS_UPDATE"
            }

            try:
                response = requests.post(EVENTS_URL, json=event, timeout=3)
                if response.status_code == 200:
                    resp_json = response.json()
                    status = resp_json.get("status", "OK")
                    order_tag = vehicle.get("order_id", "N/A")
                    print(
                        f"[{vehicle_id}] Order: {order_tag} | Speed: {speed:.0f} km/h | "
                        f"Hdg: {heading:.0f}° | Status: {status} | Kafka: STREAMING"
                    )
            except requests.RequestException:
                pass

        if iteration % 5 == 0:
            stats = kafka_producer.get_stats()
            print(f"[KAFKA STATS] Total Published: {stats['total_published']} | By Topic: {stats['published_by_topic']}")

        time.sleep(5)

if __name__ == "__main__":
    run_simulator_loop()
