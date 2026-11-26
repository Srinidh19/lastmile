"""
LastMile AI - Order Management & Tracking Engine
Provides full lifecycle management, validation, realistic generation,
fleet assignment, and telemetry-linked tracking for delivery orders.
"""

import math
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from services.routing_service import haversine_distance

# ============================================================
# CONSTANTS & METRO NCR GEO DATA POOLS
# ============================================================

VALID_STATUSES = ["PENDING", "ASSIGNED", "PICKED_UP", "IN_TRANSIT", "DELIVERED", "DELAYED"]
VALID_PRIORITIES = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]

DEFAULT_FLEET_CAPACITY = {
    "V101": 10,
    "V102": 10,
    "V103": 10,
    "V104": 10,
    "V105": 10,
}

# Realistic Delhi NCR fulfillment hubs (Pickups)
DELHI_PICKUP_HUBS = [
    {"name": "Connaught Place Fulfillment Hub, Central Delhi", "lat": 28.6315, "lon": 77.2167},
    {"name": "Okhla Industrial Freight Depot, South Delhi", "lat": 28.5355, "lon": 77.2732},
    {"name": "Naraina Cargo Logistics Center, West Delhi", "lat": 28.6285, "lon": 77.1388},
    {"name": "Kashmiri Gate Gateway Station, North Delhi", "lat": 28.6675, "lon": 77.2285},
    {"name": "Patparganj Commercial Hub, East Delhi", "lat": 28.6280, "lon": 77.3110},
    {"name": "Azadpur Regional Distribution Hub, North Delhi", "lat": 28.7155, "lon": 77.1770},
    {"name": "Kirti Nagar Transit Facility, West Delhi", "lat": 28.6550, "lon": 77.1400},
    {"name": "Nehru Place Express Depot, South Delhi", "lat": 28.5494, "lon": 77.2526},
]

# Realistic Delhi NCR delivery dropoff destinations
DELHI_DESTINATIONS = [
    {"name": "Model Town Logistics Centre, North Delhi", "lat": 28.7025, "lon": 77.1930},
    {"name": "Shahdara Delivery Point, East Delhi", "lat": 28.6730, "lon": 77.2915},
    {"name": "Rohini Sector 10 Retail Hub, North West Delhi", "lat": 28.7180, "lon": 77.1180},
    {"name": "Saket District Centre, South Delhi", "lat": 28.5245, "lon": 77.2160},
    {"name": "Dwarka Sector 6 Commercial Node, South West Delhi", "lat": 28.5920, "lon": 77.0680},
    {"name": "Dilshad Garden Distribution Hub, East Delhi", "lat": 28.6765, "lon": 77.3210},
    {"name": "Netaji Subhash Place Pitampura, North Delhi", "lat": 28.6940, "lon": 77.1490},
    {"name": "Vasant Kunj Sector C Parcel Annex, South Delhi", "lat": 28.5290, "lon": 77.1510},
    {"name": "Laxmi Nagar Commercial Center, East Delhi", "lat": 28.6305, "lon": 77.2775},
    {"name": "Karol Bagh Sorting Facility, Central Delhi", "lat": 28.6515, "lon": 77.1905},
    {"name": "Hauz Khas Enclave Delivery Station, South Delhi", "lat": 28.5490, "lon": 77.2060},
    {"name": "Janakpuri District Centre, West Delhi", "lat": 28.6290, "lon": 77.0780},
    {"name": "Mayur Vihar Phase 1 Parcel Office, East Delhi", "lat": 28.6080, "lon": 77.2960},
    {"name": "Lajpat Nagar Central Logistics, South Delhi", "lat": 28.5680, "lon": 77.2435},
    {"name": "Punjabi Bagh Commercial Point, West Delhi", "lat": 28.6685, "lon": 77.1270},
    {"name": "Civil Lines Transit Station, North Delhi", "lat": 28.6810, "lon": 77.2230},
    {"name": "Paschim Vihar Delivery Station, West Delhi", "lat": 28.6690, "lon": 77.1020},
    {"name": "Greater Kailash 1 Retail Annex, South Delhi", "lat": 28.5480, "lon": 77.2340},
    {"name": "Rajouri Garden Commercial Hub, West Delhi", "lat": 28.6470, "lon": 77.1210},
    {"name": "Preet Vihar Logistics Point, East Delhi", "lat": 28.6390, "lon": 77.2980},
    {"name": "Loni Border Commercial Center, Ghaziabad Border", "lat": 28.7400, "lon": 77.3800},
    {"name": "Seelampur Retail Annex, North East Delhi", "lat": 28.7100, "lon": 77.3650},
    {"name": "Pragati Maidan Depot, Central Delhi", "lat": 28.6200, "lon": 77.2150},
    {"name": "ISBT Kashmiri Gate Parcel Hub, North Delhi", "lat": 28.6500, "lon": 77.2400},
]

PACKAGE_TYPES = [
    "Critical Medical Diagnostics",
    "Express Pharmaceutical Supplies",
    "High-Value Electronics & Chips",
    "Urgent Legal & Financial Contracts",
    "Automotive Precision Components",
    "Temperature-Sensitive Biologics",
    "E-Commerce Priority Fast-Track",
    "Industrial Laboratory Equipment",
    "Telecom Hardware Spares",
    "Retail Consumer Electronics",
]

def _now() -> datetime:
    return datetime.now(timezone.utc)

def _now_iso() -> str:
    return _now().isoformat()


# ============================================================
# ORDER DATA MODEL VALIDATION & NORMALIZATION
# ============================================================

def validate_and_normalize_order(order_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validates all mandatory fields for an order and returns a strictly typed dictionary.
    Guarantees both required flat fields:
      - order_id
      - pickup_latitude
      - pickup_longitude
      - destination_latitude
      - destination_longitude
      - assigned_vehicle_id
      - priority
      - created_at
      - delivery_deadline
      - estimated_delivery_time
      - order_status
      - actual_delivery_time
    as well as structured helper properties for frontend and backward compatibility.
    """
    if not isinstance(order_data, dict):
        raise ValueError("Order data must be a dictionary")

    order_id = str(order_data.get("order_id", "")).strip()
    if not order_id:
        raise ValueError("order_id is required")

    # Pickup coordinates (accept flat or nested)
    if "pickup_latitude" in order_data and "pickup_longitude" in order_data:
        pickup_lat = float(order_data["pickup_latitude"])
        pickup_lon = float(order_data["pickup_longitude"])
    elif "pickup_location" in order_data and isinstance(order_data["pickup_location"], dict):
        pickup_lat = float(order_data["pickup_location"]["lat"])
        pickup_lon = float(order_data["pickup_location"]["lon"])
    else:
        raise ValueError("pickup_latitude and pickup_longitude are required")

    # Destination coordinates (accept flat or nested)
    if "destination_latitude" in order_data and "destination_longitude" in order_data:
        dest_lat = float(order_data["destination_latitude"])
        dest_lon = float(order_data["destination_longitude"])
    elif "destination" in order_data and isinstance(order_data["destination"], dict):
        dest_lat = float(order_data["destination"]["lat"])
        dest_lon = float(order_data["destination"]["lon"])
    else:
        raise ValueError("destination_latitude and destination_longitude are required")

    if not (-90.0 <= pickup_lat <= 90.0 and -90.0 <= dest_lat <= 90.0):
        raise ValueError("Latitudes must be between -90 and 90")
    if not (-180.0 <= pickup_lon <= 180.0 and -180.0 <= dest_lon <= 180.0):
        raise ValueError("Longitudes must be between -180 and 180")

    # Assigned vehicle
    raw_vid = order_data.get("assigned_vehicle_id")
    assigned_vehicle_id = str(raw_vid).strip() if raw_vid and str(raw_vid).strip().upper() not in ["NONE", "NULL", ""] else None

    # Priority
    priority = str(order_data.get("priority", "MEDIUM")).strip().upper()
    if priority not in VALID_PRIORITIES:
        priority = "MEDIUM"

    # Status
    order_status = str(order_data.get("order_status", "PENDING" if not assigned_vehicle_id else "ASSIGNED")).strip().upper()
    if order_status not in VALID_STATUSES:
        order_status = "ASSIGNED" if assigned_vehicle_id else "PENDING"

    # Timestamps
    now = _now()
    created_at = str(order_data.get("created_at") or now.isoformat())
    
    # Deadline
    raw_deadline = order_data.get("delivery_deadline")
    if not raw_deadline:
        deadline_dt = now + timedelta(minutes=60)
        delivery_deadline = deadline_dt.isoformat()
    else:
        delivery_deadline = str(raw_deadline)

    # Estimated delivery time
    raw_eta = order_data.get("estimated_delivery_time")
    if not raw_eta:
        direct_dist = haversine_distance(pickup_lat, pickup_lon, dest_lat, dest_lon)
        est_minutes = max(10, round((direct_dist * 1.30 / 28.0) * 60))
        estimated_delivery_time = (now + timedelta(minutes=est_minutes)).isoformat()
    else:
        estimated_delivery_time = str(raw_eta)

    # Actual delivery time
    actual_delivery_time = order_data.get("actual_delivery_time")
    if order_status == "DELIVERED" and not actual_delivery_time:
        actual_delivery_time = now.isoformat()
    elif actual_delivery_time:
        actual_delivery_time = str(actual_delivery_time)
    else:
        actual_delivery_time = None

    # Addresses & descriptions
    pickup_address = order_data.get("pickup_address") or (order_data.get("pickup_location", {}).get("address") if isinstance(order_data.get("pickup_location"), dict) else f"Pickup Point ({pickup_lat:.4f}, {pickup_lon:.4f})")
    dest_address = order_data.get("destination_address") or (order_data.get("destination", {}).get("address") if isinstance(order_data.get("destination"), dict) else f"Delivery Point ({dest_lat:.4f}, {dest_lon:.4f})")
    package_desc = order_data.get("package_description", "Standard Cargo Package")

    return {
        # Flat model fields (per specification)
        "order_id": order_id,
        "pickup_latitude": round(pickup_lat, 6),
        "pickup_longitude": round(pickup_lon, 6),
        "destination_latitude": round(dest_lat, 6),
        "destination_longitude": round(dest_lon, 6),
        "assigned_vehicle_id": assigned_vehicle_id,
        "priority": priority,
        "created_at": created_at,
        "delivery_deadline": delivery_deadline,
        "estimated_delivery_time": estimated_delivery_time,
        "order_status": order_status,
        "actual_delivery_time": actual_delivery_time,
        # Helper fields for UI & backward compatibility
        "pickup_address": pickup_address,
        "destination_address": dest_address,
        "package_description": package_desc,
        "pickup_location": {
            "lat": round(pickup_lat, 6),
            "lon": round(pickup_lon, 6),
            "address": pickup_address
        },
        "destination": {
            "lat": round(dest_lat, 6),
            "lon": round(dest_lon, 6),
            "address": dest_address
        },
        "updated_at": order_data.get("updated_at") or now.isoformat()
    }


# ============================================================
# IN-MEMORY DATA STORE
# ============================================================

_ORDERS_STORE: Dict[str, Dict[str, Any]] = {}


def generate_realistic_orders(
    count: int = 35,
    vehicle_capacities: Optional[Dict[str, int]] = None
) -> List[Dict[str, Any]]:
    """
    Generates 25-50 realistic delivery orders in Delhi NCR:
    - Distributed across fleet vehicles (V101 - V105)
    - Strictly respecting vehicle capacity limits
    - Leaves realistic unassigned pending orders in the queue
    - Diverse priorities (CRITICAL, HIGH, MEDIUM, LOW)
    - Realistic delivery deadlines and ETAs
    """
    if count < 25:
        count = 25
    elif count > 50:
        count = 50

    caps = vehicle_capacities or DEFAULT_FLEET_CAPACITY
    available_vehicles = list(caps.keys())
    now = _now()

    # Determine distribution: leave ~20% of orders unassigned for pending queue
    unassigned_count = max(4, round(count * 0.20))
    assigned_count = count - unassigned_count

    # Plan allocations per vehicle respecting capacity
    vehicle_allocations: Dict[str, int] = {vid: 0 for vid in available_vehicles}
    orders_to_distribute = assigned_count

    while orders_to_distribute > 0:
        for vid in available_vehicles:
            if orders_to_distribute <= 0:
                break
            if vehicle_allocations[vid] < caps.get(vid, 10):
                vehicle_allocations[vid] += 1
                orders_to_distribute -= 1

    generated: List[Dict[str, Any]] = []
    order_seq = 100

    # Build assigned orders
    for vid, num_orders in vehicle_allocations.items():
        for i in range(num_orders):
            order_seq += 1
            order_id = f"ORD-{order_seq}"

            # Pick pickup hub and destination
            pickup = random.choice(DELHI_PICKUP_HUBS)
            dest = random.choice(DELHI_DESTINATIONS)
            while dest["lat"] == pickup["lat"] and dest["lon"] == pickup["lon"]:
                dest = random.choice(DELHI_DESTINATIONS)

            # Priority distribution: 15% CRITICAL, 35% HIGH, 35% MEDIUM, 15% LOW
            p_roll = random.random()
            if p_roll < 0.15:
                priority = "CRITICAL"
                deadline_min = random.randint(25, 45)
            elif p_roll < 0.50:
                priority = "HIGH"
                deadline_min = random.randint(45, 65)
            elif p_roll < 0.85:
                priority = "MEDIUM"
                deadline_min = random.randint(65, 100)
            else:
                priority = "LOW"
                deadline_min = random.randint(100, 150)

            # Calculate route distance for realistic travel time
            dist_km = haversine_distance(pickup["lat"], pickup["lon"], dest["lat"], dest["lon"])
            travel_min = max(10, round((dist_km * 1.30 / 28.0) * 60))

            # Status assignment: First order for vehicle is IN_TRANSIT, subsequent are ASSIGNED or PICKED_UP
            if i == 0:
                status = "IN_TRANSIT"
            elif i == 1:
                status = "PICKED_UP"
            else:
                status = "ASSIGNED"

            # Create order item
            item = {
                "order_id": order_id,
                "pickup_latitude": pickup["lat"],
                "pickup_longitude": pickup["lon"],
                "pickup_address": pickup["name"],
                "destination_latitude": dest["lat"],
                "destination_longitude": dest["lon"],
                "destination_address": dest["name"],
                "assigned_vehicle_id": vid,
                "priority": priority,
                "created_at": (now - timedelta(minutes=random.randint(10, 40))).isoformat(),
                "delivery_deadline": (now + timedelta(minutes=deadline_min)).isoformat(),
                "estimated_delivery_time": (now + timedelta(minutes=travel_min)).isoformat(),
                "order_status": status,
                "actual_delivery_time": None,
                "package_description": random.choice(PACKAGE_TYPES)
            }
            generated.append(validate_and_normalize_order(item))

    # Build unassigned (PENDING) orders
    for _ in range(unassigned_count):
        order_seq += 1
        order_id = f"ORD-{order_seq}"

        pickup = random.choice(DELHI_PICKUP_HUBS)
        dest = random.choice(DELHI_DESTINATIONS)
        dist_km = haversine_distance(pickup["lat"], pickup["lon"], dest["lat"], dest["lon"])
        travel_min = max(12, round((dist_km * 1.30 / 28.0) * 60))

        priority = random.choice(["HIGH", "MEDIUM", "MEDIUM", "LOW"])
        deadline_min = random.randint(60, 120)

        item = {
            "order_id": order_id,
            "pickup_latitude": pickup["lat"],
            "pickup_longitude": pickup["lon"],
            "pickup_address": pickup["name"],
            "destination_latitude": dest["lat"],
            "destination_longitude": dest["lon"],
            "destination_address": dest["name"],
            "assigned_vehicle_id": None,
            "priority": priority,
            "created_at": (now - timedelta(minutes=random.randint(5, 20))).isoformat(),
            "delivery_deadline": (now + timedelta(minutes=deadline_min)).isoformat(),
            "estimated_delivery_time": (now + timedelta(minutes=travel_min)).isoformat(),
            "order_status": "PENDING",
            "actual_delivery_time": None,
            "package_description": random.choice(PACKAGE_TYPES)
        }
        generated.append(validate_and_normalize_order(item))

    # Overwrite in-memory store
    global _ORDERS_STORE
    _ORDERS_STORE = {ord_item["order_id"]: ord_item for ord_item in generated}
    return list(_ORDERS_STORE.values())


# Initialize default seed pool of 35 orders
_ORDERS_STORE = {o["order_id"]: o for o in generate_realistic_orders(count=35)}


# ============================================================
# ORDER RETRIEVAL & QUERYING
# ============================================================

def get_all_orders(
    status: Optional[str] = None,
    priority: Optional[str] = None,
    vehicle_id: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Retrieve all orders with optional multi-attribute filtering."""
    results = list(_ORDERS_STORE.values())

    if status:
        stat_upper = status.strip().upper()
        results = [o for o in results if o.get("order_status", "").upper() == stat_upper]

    if priority:
        pri_upper = priority.strip().upper()
        results = [o for o in results if o.get("priority", "").upper() == pri_upper]

    if vehicle_id:
        v_upper = vehicle_id.strip().upper()
        if v_upper in ["UNASSIGNED", "NONE"]:
            results = [o for o in results if not o.get("assigned_vehicle_id")]
        else:
            results = [o for o in results if (o.get("assigned_vehicle_id") or "").upper() == v_upper]

    return results


def get_order(order_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve a single order by order_id."""
    return _ORDERS_STORE.get(str(order_id).strip())


def get_orders_by_vehicle(vehicle_id: str) -> List[Dict[str, Any]]:
    """Retrieve all active/assigned orders for a specific vehicle."""
    v_clean = str(vehicle_id).strip().upper()
    return [
        o for o in _ORDERS_STORE.values()
        if (o.get("assigned_vehicle_id") or "").upper() == v_clean
    ]


# ============================================================
# ORDER ASSIGNMENT ENGINE
# ============================================================

def get_fleet_order_counts() -> Dict[str, int]:
    """Calculate the number of active (non-delivered) orders per vehicle."""
    counts: Dict[str, int] = {vid: 0 for vid in DEFAULT_FLEET_CAPACITY}
    for o in _ORDERS_STORE.values():
        vid = o.get("assigned_vehicle_id")
        if vid in counts and o.get("order_status") != "DELIVERED":
            counts[vid] += 1
    return counts


def get_available_vehicles(vehicle_capacities: Optional[Dict[str, int]] = None) -> List[Dict[str, Any]]:
    """
    Identifies vehicles that currently have spare capacity to accept more orders.
    """
    caps = vehicle_capacities or DEFAULT_FLEET_CAPACITY
    counts = get_fleet_order_counts()
    available = []

    for vid, max_cap in caps.items():
        curr_load = counts.get(vid, 0)
        spare = max_cap - curr_load
        if spare > 0:
            available.append({
                "vehicle_id": vid,
                "capacity": max_cap,
                "current_load": curr_load,
                "spare_capacity": spare
            })

    return available


def _emit_order_stream_event(order: Dict[str, Any], event_action: str):
    """Safely stream order lifecycle event to Kafka topic 'order_events'."""
    try:
        from services.streaming.producer import producer
        producer.publish_order_event(
            order_id=order["order_id"],
            event_action=event_action,
            pickup_latitude=order["pickup_latitude"],
            pickup_longitude=order["pickup_longitude"],
            destination_latitude=order["destination_latitude"],
            destination_longitude=order["destination_longitude"],
            order_status=order["order_status"],
            assigned_vehicle_id=order.get("assigned_vehicle_id"),
            priority=order.get("priority", "MEDIUM"),
            delivery_deadline=order.get("delivery_deadline"),
            estimated_delivery_time=order.get("estimated_delivery_time"),
            actual_delivery_time=order.get("actual_delivery_time")
        )
    except Exception:
        pass


def assign_order(
    order_id: str,
    vehicle_id: str,
    vehicle_capacities: Optional[Dict[str, int]] = None
) -> Dict[str, Any]:
    """
    Assigns or reassigns an order to a vehicle:
    - Validates vehicle exists and order exists
    - Prevents assignment of completed (DELIVERED) orders
    - Strictly checks vehicle capacity (rejects if capacity reached)
    - Updates order_status to ASSIGNED (or IN_TRANSIT if first active)
    - Recalculates estimated delivery time
    """
    order = get_order(order_id)
    if not order:
        raise ValueError(f"Order '{order_id}' not found")

    if order.get("order_status") == "DELIVERED":
        raise ValueError(f"Cannot reassign completed order '{order_id}'")

    caps = vehicle_capacities or DEFAULT_FLEET_CAPACITY
    v_clean = str(vehicle_id).strip().upper()
    if v_clean not in caps:
        raise ValueError(f"Vehicle '{vehicle_id}' is not registered in the fleet master")

    # Check if already assigned to this exact vehicle
    if (order.get("assigned_vehicle_id") or "").upper() == v_clean:
        return order

    # Check capacity limit
    max_cap = caps[v_clean]
    current_counts = get_fleet_order_counts()
    if current_counts.get(v_clean, 0) >= max_cap:
        raise ValueError(f"Vehicle {v_clean} is at maximum capacity ({max_cap}/{max_cap})")

    # Update assignment
    prev_vehicle = order.get("assigned_vehicle_id")
    order["assigned_vehicle_id"] = v_clean
    if order["order_status"] in ["PENDING", "DELAYED"]:
        order["order_status"] = "ASSIGNED"
    order["updated_at"] = _now_iso()

    # Recalculate ETA
    dist_km = haversine_distance(
        order["pickup_latitude"], order["pickup_longitude"],
        order["destination_latitude"], order["destination_longitude"]
    )
    est_min = max(8, round((dist_km * 1.30 / 28.0) * 60))
    order["estimated_delivery_time"] = (_now() + timedelta(minutes=est_min)).isoformat()

    action = "REASSIGNED" if (prev_vehicle and prev_vehicle != v_clean) else "ASSIGNED"
    _emit_order_stream_event(order, action)

    return order


def create_order(order_data: Dict[str, Any]) -> Dict[str, Any]:
    """Creates a new order after validating and normalizing data."""
    validated = validate_and_normalize_order(order_data)
    _ORDERS_STORE[validated["order_id"]] = validated
    _emit_order_stream_event(validated, "CREATED")
    return validated


# ============================================================
# ORDER LIFECYCLE STATE MACHINE
# ============================================================

ALLOWED_TRANSITIONS: Dict[str, List[str]] = {
    "PENDING": ["ASSIGNED"],
    "ASSIGNED": ["PENDING", "PICKED_UP", "IN_TRANSIT"],
    "PICKED_UP": ["IN_TRANSIT", "ASSIGNED"],
    "IN_TRANSIT": ["DELIVERED", "DELAYED", "ASSIGNED"],
    "DELAYED": ["IN_TRANSIT", "DELIVERED", "ASSIGNED"],
    "DELIVERED": []
}

def update_order_status(
    order_id: str,
    new_status: str,
    actual_delivery_time: Optional[str] = None
) -> Dict[str, Any]:
    """
    Updates the lifecycle status of an order with strict transition validation:
    PENDING -> ASSIGNED -> PICKED_UP -> IN_TRANSIT -> DELIVERED (or DELAYED)
    """
    order = get_order(order_id)
    if not order:
        raise ValueError(f"Order '{order_id}' not found")

    new_stat_upper = str(new_status).strip().upper().replace(" ", "_")
    if new_stat_upper not in VALID_STATUSES:
        raise ValueError(f"Invalid status '{new_status}'. Allowed: {VALID_STATUSES}")

    current_stat = order["order_status"].replace(" ", "_")
    if current_stat == new_stat_upper:
        return order

    allowed = ALLOWED_TRANSITIONS.get(current_stat, [])
    if new_stat_upper not in allowed:
        raise ValueError(
            f"Invalid status transition from '{current_stat}' to '{new_stat_upper}'. "
            f"Allowed next statuses: {allowed}"
        )

    if new_stat_upper in ["PICKED_UP", "IN_TRANSIT"] and not order.get("assigned_vehicle_id"):
        raise ValueError(f"Cannot transition order '{order_id}' to {new_stat_upper} without an assigned vehicle")

    order["order_status"] = new_stat_upper
    now = _now()
    order["updated_at"] = now.isoformat()

    if new_stat_upper == "DELIVERED":
        order["actual_delivery_time"] = actual_delivery_time or now.isoformat()
    elif new_stat_upper == "PENDING":
        order["assigned_vehicle_id"] = None
        order["actual_delivery_time"] = None

    _emit_order_stream_event(order, new_stat_upper)

    return order


def update_order_telemetry_progress(
    vehicle_id: str,
    current_lat: float,
    current_lon: float,
    is_stopped: bool = False,
    stopped_seconds: int = 0
) -> List[Dict[str, Any]]:
    """
    Triggered by GPS telemetry events:
    - Calculates distance to order destination.
    - If vehicle arrives at destination (dist <= 0.20 km / 200m), automatically marks DELIVERED.
    - If vehicle experiences critical stoppage (>= 20s), marks in-transit orders as DELAYED.
    - Dynamically updates real-time estimated_delivery_time (ETA).
    """
    now = _now()
    affected = get_orders_by_vehicle(vehicle_id)

    for order in affected:
        if order["order_status"] == "DELIVERED":
            continue

        dest_lat = order["destination_latitude"]
        dest_lon = order["destination_longitude"]
        dist_km = haversine_distance(current_lat, current_lon, dest_lat, dest_lon)

        # Arrival at destination check
        if dist_km <= 0.20:
            order["order_status"] = "DELIVERED"
            order["actual_delivery_time"] = now.isoformat()
            order["updated_at"] = now.isoformat()
            _emit_order_stream_event(order, "DELIVERED")
            continue

        # Dynamic travel estimation
        remaining_travel_minutes = max(2, round((dist_km * 1.30 / 28.0) * 60))
        if is_stopped:
            remaining_travel_minutes += round(stopped_seconds / 60)

        eta_time = now + timedelta(minutes=remaining_travel_minutes)
        order["estimated_delivery_time"] = eta_time.isoformat()
        order["updated_at"] = now.isoformat()

        # Stoppage delay tracking
        if is_stopped and stopped_seconds >= 20:
            if order["order_status"] in ["ASSIGNED", "PICKED_UP", "IN_TRANSIT"]:
                order["order_status"] = "DELAYED"
        elif order["order_status"] == "DELAYED" and not is_stopped:
            # Resumed moving
            order["order_status"] = "IN_TRANSIT"

    return affected


# ============================================================
# AGGREGATED METRICS & RECOVERY SUPPORT
# ============================================================

def get_orders_summary() -> Dict[str, Any]:
    """Returns real-time aggregated metrics across all orders."""
    all_orders = list(_ORDERS_STORE.values())
    counts = get_fleet_order_counts()

    return {
        "total_orders": len(all_orders),
        "pending": sum(1 for o in all_orders if o["order_status"] == "PENDING"),
        "assigned": sum(1 for o in all_orders if o["order_status"] == "ASSIGNED"),
        "picked_up": sum(1 for o in all_orders if o["order_status"] == "PICKED_UP"),
        "in_transit": sum(1 for o in all_orders if o["order_status"] == "IN_TRANSIT"),
        "delivered": sum(1 for o in all_orders if o["order_status"] == "DELIVERED"),
        "delayed": sum(1 for o in all_orders if o["order_status"] == "DELAYED"),
        "unassigned_orders_count": sum(1 for o in all_orders if not o.get("assigned_vehicle_id")),
        "distribution_by_vehicle": counts,
        "priority_breakdown": {
            "CRITICAL": sum(1 for o in all_orders if o.get("priority") == "CRITICAL"),
            "HIGH": sum(1 for o in all_orders if o.get("priority") == "HIGH"),
            "MEDIUM": sum(1 for o in all_orders if o.get("priority") == "MEDIUM"),
            "LOW": sum(1 for o in all_orders if o.get("priority") == "LOW")
        }
    }


def reset_seed_orders() -> None:
    """Resets orders store to fresh generated data."""
    generate_realistic_orders(count=35)
