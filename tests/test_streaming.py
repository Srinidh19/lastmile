import time
import pytest
from datetime import datetime, timezone
from fastapi.testclient import TestClient

from backend.main import app, vehicles, active_alerts, VEHICLE_MASTER
from services.streaming import (
    config,
    StreamEventEnvelope,
    VehicleTelemetryPayload,
    VehicleStatusPayload,
    OrderEventPayload,
    DisruptionEventPayload,
    RecoveryEventPayload,
    validate_stream_event,
    producer,
    consumer,
    streaming_manager
)
from services.order_service import (
    reset_seed_orders,
    get_order,
    create_order,
    assign_order,
    update_order_status
)

client = TestClient(app)

@pytest.fixture(scope="module", autouse=True)
def setup_streaming():
    """Ensure streaming manager is running during tests."""
    reset_seed_orders()
    streaming_manager.start()
    time.sleep(0.2)
    yield
    streaming_manager.stop()

def test_1_and_2_streaming_infrastructure_and_condense_config():
    """Verify Kafka infrastructure configuration and Condense connectivity settings."""
    overview = streaming_manager.get_overview()
    assert overview["status"] == "HEALTHY"
    assert "active_topics" in overview
    assert len(overview["active_topics"]) == 5
    assert "vehicle_telemetry" in overview["active_topics"]
    assert "disruption_events" in overview["active_topics"]
    assert "order_events" in overview["active_topics"]
    assert overview["consumer_group"] == config.consumer_group
    assert overview["streaming_mode"] in ["CONFLUENT_KAFKA", "IN_MEMORY"]

def test_3_event_schemas_and_validation():
    """Verify CloudEvents envelope, schema validation, and event versioning."""
    envelope = producer.publish_vehicle_telemetry(
        vehicle_id="V101",
        latitude=28.6139,
        longitude=77.2090,
        speed=32.5,
        heading=45.0,
        vehicle_status="MOVING"
    )
    assert envelope.event_id is not None
    assert envelope.schema_version == "1.0"
    assert envelope.entity_id == "V101"
    assert envelope.event_payload["speed"] == 32.5
    assert envelope.event_payload["heading"] == 45.0

    # Test invalid schema rejection
    with pytest.raises(Exception):
        validate_stream_event({
            "event_id": "bad",
            "event_type": "vehicle_telemetry",
            "timestamp": "now",
            "entity_id": "V101",
            "schema_version": "1.0",
            "event_payload": {"vehicle_id": "V101", "latitude": 999.0}  # Invalid latitude > 90
        })

def test_4_and_5_publish_and_consume_vehicle_telemetry():
    """Verify vehicle telemetry publish, consume, and deduplication."""
    initial_consumed = consumer.consumed_counts.get("vehicle_telemetry", 0)
    
    # Publish telemetry for V103
    env = producer.publish_vehicle_telemetry(
        vehicle_id="V103",
        latitude=28.6310,
        longitude=77.2210,
        speed=28.0,
        heading=90.0,
        vehicle_status="MOVING"
    )
    assert env is not None

    # Wait for consumer thread to dispatch
    time.sleep(0.3)
    after_consumed = consumer.consumed_counts.get("vehicle_telemetry", 0)
    assert after_consumed > initial_consumed

def test_6_update_backend_vehicle_state_from_telemetry():
    """Verify that consumed telemetry updates backend vehicle registry accurately."""
    # Publish distinct coordinates for V104
    test_lat = 28.6455
    test_lon = 77.2355
    test_speed = 36.0
    producer.publish_vehicle_telemetry(
        vehicle_id="V104",
        latitude=test_lat,
        longitude=test_lon,
        speed=test_speed,
        heading=180.0,
        vehicle_status="MOVING"
    )
    time.sleep(0.3)

    # Check vehicle state in backend
    resp = client.get("/vehicles")
    assert resp.status_code == 200
    vehicles_data = resp.json()
    v104 = next((v for v in vehicles_data if v["vehicle_id"] == "V104"), None)
    assert v104 is not None
    assert v104["latitude"] == test_lat
    assert v104["longitude"] == test_lon
    assert v104["speed"] == test_speed
    assert v104["status"] == "MOVING"

def test_7_publish_and_consume_order_events():
    """Verify order lifecycle streaming on creation, assignment, and status update."""
    initial_order_consumed = consumer.consumed_counts.get("order_events", 0)

    # 1. Create order
    new_order = {
        "order_id": "ORD-STREAM-901",
        "pickup_latitude": 28.6315,
        "pickup_longitude": 77.2167,
        "destination_latitude": 28.7025,
        "destination_longitude": 77.1930,
        "priority": "HIGH"
    }
    created = create_order(new_order)
    assert created["order_id"] == "ORD-STREAM-901"

    # 2. Assign order
    assigned = assign_order("ORD-STREAM-901", "V101")
    assert assigned["assigned_vehicle_id"] == "V101"

    # 3. Status progression
    in_transit = update_order_status("ORD-STREAM-901", "IN_TRANSIT")
    assert in_transit["order_status"] == "IN_TRANSIT"

    # Allow consumer thread to process
    time.sleep(0.3)
    after_order_consumed = consumer.consumed_counts.get("order_events", 0)
    assert after_order_consumed >= initial_order_consumed + 3

def test_8_and_9_publish_and_consume_disruption_event():
    """Verify disruption event generation, Kafka publishing, and backend alert generation."""
    # Simulate V102 critical stoppage
    stop_event = {
        "vehicle_id": "V102",
        "latitude": 28.6200,
        "longitude": 77.2150,
        "speed": 0.0,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    # First tick: stopped since starts
    resp1 = client.post("/events", json=stop_event)
    assert resp1.status_code == 200

    # Artificially set stopped_since in the past to trigger CRITICAL threshold (>=20s)
    vehicles["V102"]["stopped_since"] = datetime.fromisoformat("2026-11-26T10:00:00+00:00")
    vehicles["V102"]["stopped_since_wall"] = datetime.fromisoformat("2026-11-26T10:00:00+00:00")

    # Second tick: critical disruption should be published
    stop_event2 = {
        "vehicle_id": "V102",
        "latitude": 28.6200,
        "longitude": 77.2150,
        "speed": 0.0,
        "timestamp": "2026-11-26T10:00:30+00:00"
    }
    resp2 = client.post("/events", json=stop_event2)
    assert resp2.status_code == 200
    assert resp2.json()["active_alert"] is True
    assert "V102" in active_alerts
    assert active_alerts["V102"]["severity"] == "CRITICAL"

    # Verify duplicate prevention: third tick should NOT re-publish disruption event
    initial_disrupt_count = producer.published_counts.get("disruption_events", 0)
    resp3 = client.post("/events", json=stop_event2)
    assert resp3.status_code == 200
    after_disrupt_count = producer.published_counts.get("disruption_events", 0)
    assert after_disrupt_count == initial_disrupt_count

def test_10_dashboard_streaming_status_endpoint():
    """Verify that backend provides live streaming status and event telemetry for dashboard."""
    resp = client.get("/streaming/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "HEALTHY"
    assert "producer_stats" in data
    assert "consumer_stats" in data
    assert data["producer_stats"]["total_published"] > 0
    assert data["consumer_stats"]["total_consumed"] > 0
    assert len(data["recent_events"]) > 0

    # Verify events listing endpoint
    events_resp = client.get("/streaming/events?limit=10")
    assert events_resp.status_code == 200
    events = events_resp.json()
    assert isinstance(events, list)
    assert len(events) > 0
