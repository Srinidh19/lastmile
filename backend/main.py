import math
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from services.routing_service import get_road_route, haversine_distance
from services.order_service import (
    get_all_orders,
    get_order,
    get_orders_by_vehicle,
    create_order,
    assign_order,
    update_order_status,
    update_order_telemetry_progress,
    generate_realistic_orders,
    get_orders_summary,
    get_available_vehicles,
    get_fleet_order_counts,
    reset_seed_orders
)
from services.streaming import (
    config as streaming_config,
    StreamEventEnvelope,
    validate_stream_event,
    producer as stream_producer,
    consumer as stream_consumer,
    streaming_manager
)

app = FastAPI(
    title="LastMile AI - Mobility & Order Management Intelligence",
    description="Real-time fleet monitoring, delay detection, order tracking lifecycle and recovery recommendations",
    version="1.2.0"
)

# =====================================================
# 1. IN-MEMORY STORAGE & FLEET MASTER
# =====================================================

vehicles: Dict[str, Dict[str, Any]] = {}
event_history: List[Dict[str, Any]] = []
active_alerts: Dict[str, Dict[str, Any]] = {}

# Synthetic fleet master data
VEHICLE_MASTER = {
    "V101": {"capacity": 10},
    "V102": {"capacity": 10},
    "V103": {"capacity": 10},
    "V104": {"capacity": 10},
    "V105": {"capacity": 10}
}

FLEET_CAPACITIES = {vid: data["capacity"] for vid, data in VEHICLE_MASTER.items()}

# Alert thresholds (seconds)
MEDIUM_STOP_THRESHOLD = 10
CRITICAL_STOP_THRESHOLD = 20

# =====================================================
# 2. HELPER FUNCTIONS
# =====================================================

def current_time():
    return datetime.now(timezone.utc)


def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate straight-line distance using Haversine formula."""
    return haversine_distance(lat1, lon1, lat2, lon2)


# Pydantic Schemas for Request Validation
class OrderCreateRequest(BaseModel):
    order_id: str
    pickup_latitude: float = Field(..., ge=-90, le=90)
    pickup_longitude: float = Field(..., ge=-180, le=180)
    destination_latitude: float = Field(..., ge=-90, le=90)
    destination_longitude: float = Field(..., ge=-180, le=180)
    assigned_vehicle_id: Optional[str] = None
    priority: Optional[str] = "MEDIUM"
    delivery_deadline: Optional[str] = None
    estimated_delivery_time: Optional[str] = None
    order_status: Optional[str] = "PENDING"
    pickup_address: Optional[str] = None
    destination_address: Optional[str] = None
    package_description: Optional[str] = "Standard Cargo Package"


class OrderStatusUpdateRequest(BaseModel):
    status: str
    actual_delivery_time: Optional[str] = None


# =====================================================
# 3. HOME AND HEALTH
# =====================================================

@app.get("/")
def home():
    return {
        "application": "LastMile AI",
        "status": "online",
        "version": "1.2.0",
        "message": "Mobility intelligence and order management engine running"
    }


@app.get("/health")
def health():
    orders_stat = get_orders_summary()
    return {
        "status": "healthy",
        "vehicles_connected": len(vehicles),
        "active_alerts": len(active_alerts),
        "total_orders": orders_stat["total_orders"],
        "orders_in_transit": orders_stat["in_transit"],
        "orders_delivered": orders_stat["delivered"],
        "unassigned_orders": orders_stat["unassigned_orders_count"]
    }


# =====================================================
# 4. VEHICLE EVENT INGESTION & ORDER SYNCHRONIZATION
# =====================================================

def process_telemetry(
    vehicle_id: str,
    latitude: float,
    longitude: float,
    speed: float,
    event_time: datetime,
    heading: float = 0.0,
    raw_event: Optional[dict] = None
) -> dict:
    """
    Core business service for processing vehicle telemetry.
    Can be invoked by HTTP endpoint, Kafka consumer, or simulator.
    """
    now = current_time()

    # Initialize vehicle state
    if vehicle_id not in vehicles:
        vehicles[vehicle_id] = {
            "vehicle_id": vehicle_id,
            "stopped_since": None,
            "risk_score": 0,
            "status": "UNKNOWN",
            "heading": heading
        }

    vehicle = vehicles[vehicle_id]
    prev_status = vehicle.get("status", "UNKNOWN")

    # Update latest vehicle information
    vehicle.update({
        "latitude": latitude,
        "longitude": longitude,
        "speed": speed,
        "heading": heading,
        "timestamp": event_time.isoformat(),
        "last_updated": now.isoformat()
    })

    # =================================================
    # 5. DELAY / STAGNATION DETECTION
    # =================================================

    is_stopped = speed <= 2
    stopped_seconds = 0

    if is_stopped:
        if vehicle["stopped_since"] is None:
            vehicle["stopped_since"] = event_time
            vehicle["stopped_since_wall"] = now

        event_stopped_sec = max(0.0, (event_time - vehicle["stopped_since"]).total_seconds())
        wall_stopped_sec = max(0.0, (now - vehicle.get("stopped_since_wall", now)).total_seconds())
        stopped_seconds = max(event_stopped_sec, wall_stopped_sec)

        vehicle["status"] = "STOPPED"
        vehicle["stopped_seconds"] = round(stopped_seconds)

        if stopped_seconds >= CRITICAL_STOP_THRESHOLD:
            severity = "CRITICAL"
            risk_score = 90
        elif stopped_seconds >= MEDIUM_STOP_THRESHOLD:
            severity = "MEDIUM"
            risk_score = 50
        else:
            severity = "LOW"
            risk_score = 10

        vehicle["risk_score"] = risk_score

        if severity in ["MEDIUM", "CRITICAL"]:
            existing_alert = active_alerts.get(vehicle_id)
            active_alerts[vehicle_id] = {
                "vehicle_id": vehicle_id,
                "alert_type": "VEHICLE_STAGNATION",
                "severity": severity,
                "risk_score": risk_score,
                "stopped_seconds": round(stopped_seconds),
                "message": f"Vehicle stationary for {round(stopped_seconds)} seconds",
                "created_at": (
                    existing_alert["created_at"]
                    if existing_alert
                    else now.isoformat()
                ),
                "updated_at": now.isoformat()
            }

            # Avoid duplicate disruption events: emit only once per stoppage episode
            if not vehicle.get("active_disruption_published"):
                vehicle["active_disruption_published"] = True
                vehicle_orders = get_orders_by_vehicle(vehicle_id)
                affected_ids = [o["order_id"] for o in vehicle_orders if o.get("order_status") != "DELIVERED"]
                stream_producer.publish_disruption_event(
                    vehicle_id=vehicle_id,
                    disruption_type="STOPPAGE",
                    severity=severity,
                    latitude=latitude,
                    longitude=longitude,
                    stoppage_duration_seconds=round(stopped_seconds),
                    affected_order_ids=affected_ids
                )
                stream_producer.publish_vehicle_status(
                    vehicle_id=vehicle_id,
                    previous_status=prev_status,
                    new_status="STOPPED",
                    reason=f"Stationary for {round(stopped_seconds)}s",
                    speed=speed
                )
    else:
        # Vehicle resumed movement
        was_disrupted = vehicle.get("active_disruption_published", False)
        vehicle["stopped_since"] = None
        vehicle["stopped_since_wall"] = None
        vehicle["stopped_seconds"] = 0
        vehicle["risk_score"] = 0
        vehicle["status"] = "MOVING"
        vehicle["active_disruption_published"] = False
        active_alerts.pop(vehicle_id, None)

        if was_disrupted or prev_status == "STOPPED":
            stream_producer.publish_vehicle_status(
                vehicle_id=vehicle_id,
                previous_status="STOPPED",
                new_status="MOVING",
                reason="Resumed movement after stoppage",
                speed=speed
            )

    # =================================================
    # 6. INTEGRATE ORDERS TELEMETRY UPDATE
    # =================================================
    affected_orders = update_order_telemetry_progress(
        vehicle_id=vehicle_id,
        current_lat=latitude,
        current_lon=longitude,
        is_stopped=is_stopped,
        stopped_seconds=round(stopped_seconds)
    )

    # Record event history
    stored_event = raw_event or {
        "vehicle_id": vehicle_id,
        "latitude": latitude,
        "longitude": longitude,
        "speed": speed,
        "heading": heading,
        "timestamp": event_time.isoformat()
    }
    event_history.append({
        **stored_event,
        "received_at": now.isoformat()
    })

    if len(event_history) > 5000:
        event_history.pop(0)

    return {
        "message": "Event received and processed",
        "vehicle_id": vehicle_id,
        "status": vehicle["status"],
        "risk_score": vehicle["risk_score"],
        "active_alert": vehicle_id in active_alerts,
        "assigned_orders_count": len(affected_orders),
        "affected_order_ids": [o["order_id"] for o in affected_orders]
    }


@app.post("/events")
def receive_event(event: dict):
    required_fields = [
        "vehicle_id",
        "latitude",
        "longitude",
        "speed",
        "timestamp"
    ]

    missing = [field for field in required_fields if field not in event]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing fields: {missing}"
        )

    vehicle_id = event["vehicle_id"]
    if vehicle_id not in VEHICLE_MASTER:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown vehicle ID '{vehicle_id}'. Must be one of {list(VEHICLE_MASTER.keys())}"
        )

    try:
        latitude = float(event["latitude"])
        longitude = float(event["longitude"])
        speed = float(event["speed"])
        heading = float(event.get("heading", 0.0))

        if not -90 <= latitude <= 90:
            raise ValueError("Invalid latitude")
        if not -180 <= longitude <= 180:
            raise ValueError("Invalid longitude")
        if speed < 0:
            raise ValueError("Speed cannot be negative")

        event_time = datetime.fromisoformat(
            str(event["timestamp"]).replace("Z", "+00:00")
        )
        if event_time.tzinfo is None:
            raise ValueError("Timestamp must include timezone")

    except (ValueError, TypeError) as error:
        raise HTTPException(
            status_code=400,
            detail=str(error)
        )

    # Process through core business engine
    result = process_telemetry(
        vehicle_id=vehicle_id,
        latitude=latitude,
        longitude=longitude,
        speed=speed,
        event_time=event_time,
        heading=heading,
        raw_event=event
    )

    # Mirror to Kafka vehicle_telemetry stream
    try:
        stream_producer.publish_vehicle_telemetry(
            vehicle_id=vehicle_id,
            latitude=latitude,
            longitude=longitude,
            speed=speed,
            heading=heading,
            vehicle_status=result["status"]
        )
    except Exception:
        pass

    return result


# =====================================================
# 7. VEHICLE INFORMATION & FLEET STATUS
# =====================================================

@app.get("/vehicles")
def get_vehicles():
    """Returns all fleet vehicles merged with live order counts and capacity."""
    results = []
    fleet_counts = get_fleet_order_counts()

    # Include all 5 vehicles from master even before telemetry received
    for vehicle_id, master in VEHICLE_MASTER.items():
        v_live = vehicles.get(vehicle_id, {
            "vehicle_id": vehicle_id,
            "status": "IDLE",
            "speed": 0,
            "latitude": None,
            "longitude": None,
            "risk_score": 0,
            "stopped_seconds": 0,
            "last_updated": None
        })

        vehicle_orders = get_orders_by_vehicle(vehicle_id)
        active_orders = [o for o in vehicle_orders if o.get("order_status") != "DELIVERED"]
        current_load = len(active_orders)
        spare_capacity = max(0, master["capacity"] - current_load)

        results.append({
            **v_live,
            "capacity": master["capacity"],
            "current_load": current_load,
            "pending_deliveries": current_load,
            "spare_capacity": spare_capacity,
            "assigned_order_ids": [o["order_id"] for o in vehicle_orders],
            "active_order_ids": [o["order_id"] for o in active_orders]
        })

    return results


@app.get("/vehicles/{vehicle_id}")
def get_vehicle(vehicle_id: str):
    if vehicle_id not in VEHICLE_MASTER:
        raise HTTPException(
            status_code=404,
            detail=f"Vehicle '{vehicle_id}' not found in fleet master"
        )

    master = VEHICLE_MASTER[vehicle_id]
    v_live = vehicles.get(vehicle_id, {
        "vehicle_id": vehicle_id,
        "status": "IDLE",
        "speed": 0,
        "latitude": None,
        "longitude": None,
        "risk_score": 0,
        "stopped_seconds": 0,
        "last_updated": None
    })

    vehicle_orders = get_orders_by_vehicle(vehicle_id)
    active_orders = [o for o in vehicle_orders if o.get("order_status") != "DELIVERED"]
    current_load = len(active_orders)
    spare_capacity = max(0, master["capacity"] - current_load)

    return {
        **v_live,
        "capacity": master["capacity"],
        "current_load": current_load,
        "pending_deliveries": current_load,
        "spare_capacity": spare_capacity,
        "assigned_orders": vehicle_orders,
        "active_order_ids": [o["order_id"] for o in active_orders]
    }


# =====================================================
# 8. ORDER MANAGEMENT ENGINE ENDPOINTS
# =====================================================

@app.post("/orders/generate")
def generate_orders_endpoint(count: int = Query(35, ge=25, le=50, description="Number of orders to generate (25-50)")):
    """
    Generates a realistic set of 25-50 delivery orders in Delhi NCR,
    distributed across fleet vehicles respecting capacity, with realistic
    unassigned pending orders in the queue.
    """
    generated = generate_realistic_orders(count=count, vehicle_capacities=FLEET_CAPACITIES)
    summary = get_orders_summary()
    return {
        "message": f"Successfully generated {len(generated)} orders",
        "count": len(generated),
        "summary": summary
    }


@app.get("/orders")
def list_orders(
    status: Optional[str] = Query(None, description="Filter by order status (PENDING, ASSIGNED, PICKED_UP, IN_TRANSIT, DELIVERED, DELAYED)"),
    priority: Optional[str] = Query(None, description="Filter by priority (CRITICAL, HIGH, MEDIUM, LOW)"),
    vehicle_id: Optional[str] = Query(None, description="Filter by assigned vehicle ID (e.g. V101, or UNASSIGNED)")
):
    """Retrieves all orders with optional filtering by status, priority, and assigned vehicle."""
    return get_all_orders(status=status, priority=priority, vehicle_id=vehicle_id)


@app.get("/orders/summary")
def get_orders_summary_endpoint():
    """Returns aggregated metrics, distribution by vehicle, and priority breakdowns."""
    return get_orders_summary()


@app.get("/orders/available-vehicles")
def get_available_vehicles_endpoint():
    """Returns list of vehicles currently possessing spare capacity for assignment."""
    return get_available_vehicles(vehicle_capacities=FLEET_CAPACITIES)


@app.get("/orders/{order_id}")
def get_order_by_id(order_id: str):
    """Retrieves single order details by unique order_id."""
    order = get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail=f"Order '{order_id}' not found")
    return order


@app.get("/orders/vehicle/{vehicle_id}")
def get_orders_for_vehicle(vehicle_id: str):
    """Retrieves all delivery orders assigned to a specific vehicle."""
    if vehicle_id.upper() not in VEHICLE_MASTER and vehicle_id.upper() not in ["UNASSIGNED", "NONE"]:
        raise HTTPException(status_code=404, detail=f"Vehicle '{vehicle_id}' not found")
    return get_orders_by_vehicle(vehicle_id)


@app.post("/orders")
def add_order(order_request: OrderCreateRequest):
    """Creates a new delivery order with strict validation."""
    try:
        new_order = create_order(order_request.dict())
        return {
            "message": "Order created successfully",
            "order": new_order
        }
    except ValueError as err:
        raise HTTPException(status_code=400, detail=str(err))


@app.post("/orders/{order_id}/assign")
def assign_order_endpoint(order_id: str, vehicle_id: str = Query(..., description="Target vehicle ID (e.g. V101)")):
    """
    Assigns or reassigns an order to an available vehicle.
    Validates fleet existence and capacity limits.
    """
    try:
        updated = assign_order(order_id=order_id, vehicle_id=vehicle_id, vehicle_capacities=FLEET_CAPACITIES)
        return {
            "message": f"Order {order_id} assigned to vehicle {vehicle_id}",
            "order": updated
        }
    except ValueError as err:
        raise HTTPException(status_code=400, detail=str(err))


@app.post("/orders/{order_id}/reassign")
def reassign_order_endpoint(order_id: str, new_vehicle_id: str = Query(..., description="Target vehicle ID")):
    """Alias for order assignment (preserves backward compatibility)."""
    return assign_order_endpoint(order_id=order_id, vehicle_id=new_vehicle_id)


@app.patch("/orders/{order_id}/status")
def update_order_status_endpoint(order_id: str, payload: OrderStatusUpdateRequest):
    """
    Updates the lifecycle status of an order (e.g. PENDING -> ASSIGNED -> PICKED_UP -> IN_TRANSIT -> DELIVERED).
    """
    try:
        updated = update_order_status(
            order_id=order_id,
            new_status=payload.status,
            actual_delivery_time=payload.actual_delivery_time
        )
        return {
            "message": f"Order {order_id} status updated to {updated['order_status']}",
            "order": updated
        }
    except ValueError as err:
        raise HTTPException(status_code=400, detail=str(err))


@app.post("/orders/reset")
def reset_orders_endpoint():
    """Resets orders store to fresh generated orders."""
    reset_seed_orders()
    return {
        "message": "Order store reset to fresh seed orders",
        "summary": get_orders_summary()
    }


# =====================================================
# 9. ROUTE GENERATION ENDPOINT (OSRM + FALLBACK)
# =====================================================

@app.get("/routes/{vehicle_id}")
def get_vehicle_route(vehicle_id: str):
    """
    Returns the road route from the vehicle's current position to its assigned order destination.
    Uses OSRM driving service with straight-line fallback.
    """
    if vehicle_id not in vehicles or vehicles[vehicle_id].get("latitude") is None:
        raise HTTPException(status_code=404, detail=f"No telemetry available for vehicle '{vehicle_id}'")

    vehicle = vehicles[vehicle_id]
    vehicle_orders = get_orders_by_vehicle(vehicle_id)

    if not vehicle_orders:
        raise HTTPException(status_code=404, detail=f"No active orders assigned to {vehicle_id}")

    # Use first active delivery order
    active_order = next((o for o in vehicle_orders if o["order_status"] != "DELIVERED"), vehicle_orders[0])
    dest_lat = active_order["destination_latitude"]
    dest_lon = active_order["destination_longitude"]

    route_info = get_road_route(
        start_lat=vehicle["latitude"],
        start_lon=vehicle["longitude"],
        end_lat=dest_lat,
        end_lon=dest_lon
    )

    return {
        "vehicle_id": vehicle_id,
        "order_id": active_order["order_id"],
        "destination_name": active_order.get("destination_address", "Destination"),
        **route_info
    }


# =====================================================
# 10. ALERT MANAGEMENT & FLEET SUMMARY
# =====================================================

@app.get("/alerts")
def get_alerts():
    return list(active_alerts.values())


@app.get("/summary")
def get_summary():
    all_vehicles = list(vehicles.values())
    orders_summary = get_orders_summary()

    return {
        "total_vehicles": len(VEHICLE_MASTER),
        "moving": sum(v.get("status") == "MOVING" for v in all_vehicles),
        "stopped": sum(v.get("status") == "STOPPED" for v in all_vehicles),
        "active_alerts": len(active_alerts),
        "critical_alerts": sum(a.get("severity") == "CRITICAL" for a in active_alerts.values()),
        "total_orders": orders_summary["total_orders"],
        "pending_orders": orders_summary["pending"],
        "assigned_orders": orders_summary["assigned"],
        "picked_up_orders": orders_summary["picked_up"],
        "orders_in_transit": orders_summary["in_transit"],
        "orders_delayed": orders_summary["delayed"],
        "orders_delivered": orders_summary["delivered"],
        "unassigned_orders": orders_summary["unassigned_orders_count"],
        "distribution_by_vehicle": orders_summary["distribution_by_vehicle"]
    }


@app.get("/events")
def get_events():
    return event_history


# =====================================================
# 11. RECOVERY ENGINE
# =====================================================

@app.get("/recovery/{vehicle_id}")
def get_recovery_recommendations(vehicle_id: str):
    if vehicle_id not in VEHICLE_MASTER:
        raise HTTPException(
            status_code=404,
            detail=f"Vehicle '{vehicle_id}' not found"
        )

    target = vehicles.get(vehicle_id)
    if not target or target.get("latitude") is None:
        return {
            "disrupted_vehicle": vehicle_id,
            "decision": "NO_TELEMETRY",
            "message": f"Vehicle {vehicle_id} has not transmitted telemetry yet."
        }

    # Recovery is only required for a stationary vehicle
    if target.get("status") != "STOPPED":
        return {
            "disrupted_vehicle": vehicle_id,
            "decision": "NO_RECOVERY_REQUIRED",
            "message": "Vehicle is currently moving or idle (not stopped)."
        }

    target_orders = get_orders_by_vehicle(vehicle_id)
    undelivered_orders = [o for o in target_orders if o["order_status"] != "DELIVERED"]
    pending = len(undelivered_orders)

    if pending == 0:
        return {
            "disrupted_vehicle": vehicle_id,
            "decision": "NO_RECOVERY_REQUIRED",
            "message": "Vehicle has no pending orders to recover."
        }

    fleet_counts = get_fleet_order_counts()
    candidates = []

    # Find eligible alternative vehicles
    for candidate_id, master in VEHICLE_MASTER.items():
        if candidate_id == vehicle_id:
            continue

        candidate_live = vehicles.get(candidate_id)
        if not candidate_live or candidate_live.get("status") != "MOVING":
            continue

        curr_load = fleet_counts.get(candidate_id, 0)
        spare_capacity = max(0, master["capacity"] - curr_load)

        if spare_capacity <= 0:
            continue

        distance = calculate_distance(
            target["latitude"],
            target["longitude"],
            candidate_live["latitude"],
            candidate_live["longitude"]
        )

        # Maximum search radius: 12 km
        if distance > 12.0:
            continue

        candidates.append({
            "vehicle_id": candidate_id,
            "distance_km": round(distance, 2),
            "spare_capacity": spare_capacity
        })

    # Rank by proximity, then spare capacity
    candidates.sort(key=lambda x: (x["distance_km"], -x["spare_capacity"]))

    remaining = pending
    recommendations = []
    order_idx = 0

    for candidate in candidates:
        if remaining <= 0:
            break

        orders_count = min(remaining, candidate["spare_capacity"])
        reassigned_slice = [
            undelivered_orders[i]["order_id"]
            for i in range(order_idx, order_idx + orders_count)
        ]
        order_idx += orders_count

        recommendations.append({
            "vehicle_id": candidate["vehicle_id"],
            "distance_km": candidate["distance_km"],
            "available_capacity": candidate["spare_capacity"],
            "orders_to_reassign": orders_count,
            "order_ids": reassigned_slice
        })

        remaining -= orders_count

    recovered = pending - remaining

    return {
        "disrupted_vehicle": vehicle_id,
        "pending_deliveries": pending,
        "disrupted_orders": [o["order_id"] for o in undelivered_orders],
        "recommendations": recommendations,
        "orders_recoverable": recovered,
        "orders_remaining": remaining,
        "decision": (
            "RECOVERY_PLAN_AVAILABLE"
            if recommendations
            else "NO_ELIGIBLE_VEHICLE_FOUND"
        ),
        "note": (
            "Select 'Execute Reassignment' in Control Tower to transfer affected orders."
        )
    }


# =====================================================
# 11. CONDENSE + KAFKA REAL-TIME STREAMING INTEGRATION
# =====================================================

def handle_stream_telemetry(envelope: StreamEventEnvelope):
    """Consumer callback for vehicle_telemetry Kafka topic."""
    payload = envelope.event_payload
    v_id = payload.get("vehicle_id")
    if not v_id or v_id not in VEHICLE_MASTER:
        return
    try:
        lat = float(payload["latitude"])
        lon = float(payload["longitude"])
        spd = float(payload["speed"])
        hdg = float(payload.get("heading", 0.0))
        ts_str = payload.get("timestamp") or current_time().isoformat()
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        process_telemetry(v_id, lat, lon, spd, dt, heading=hdg, raw_event=payload)
    except Exception:
        pass


def handle_stream_disruption(envelope: StreamEventEnvelope):
    """Consumer callback for disruption_events Kafka topic."""
    payload = envelope.event_payload
    v_id = payload.get("vehicle_id")
    if v_id in VEHICLE_MASTER and v_id in vehicles:
        vehicles[v_id]["last_disruption_event_id"] = envelope.event_id


def handle_stream_order(envelope: StreamEventEnvelope):
    """Consumer callback for order_events Kafka topic."""
    pass


def handle_stream_status(envelope: StreamEventEnvelope):
    """Consumer callback for vehicle_status Kafka topic."""
    payload = envelope.event_payload
    v_id = payload.get("vehicle_id")
    if v_id in vehicles:
        vehicles[v_id]["status"] = payload.get("new_status", vehicles[v_id]["status"])


def init_streaming_handlers():
    """Register consumer callbacks for each topic."""
    stream_consumer.register_handler(
        streaming_config.topic_vehicle_telemetry, handle_stream_telemetry
    )
    stream_consumer.register_handler(
        streaming_config.topic_disruption_events, handle_stream_disruption
    )
    stream_consumer.register_handler(
        streaming_config.topic_order_events, handle_stream_order
    )
    stream_consumer.register_handler(
        streaming_config.topic_vehicle_status, handle_stream_status
    )

# Automatically wire handlers on import
init_streaming_handlers()


@app.on_event("startup")
def startup_event():
    """Start Kafka streaming consumer and register topic handlers."""
    init_streaming_handlers()
    streaming_manager.start()


@app.on_event("shutdown")
def shutdown_event():
    """Gracefully stop streaming consumers and flush producers."""
    streaming_manager.stop()


@app.get("/streaming/status")
def get_streaming_status():
    """Returns streaming broker, producer, consumer, and topic metrics."""
    return streaming_manager.get_overview()


@app.post("/streaming/publish")
def publish_streaming_event(data: dict):
    """Allows publishing test stream events through the streaming backbone."""
    try:
        envelope = validate_stream_event(data)
        success = stream_producer.publish(envelope.event_type, envelope)
        return {
            "success": success,
            "event_id": envelope.event_id,
            "topic": envelope.event_type,
            "entity_id": envelope.entity_id
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/streaming/events")
def get_streaming_events(limit: int = Query(50, ge=1, le=200)):
    """Returns recent stream events across all topics."""
    from services.streaming.broker import in_memory_bus
    return in_memory_bus.get_history(limit=limit)
