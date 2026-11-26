import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Literal
from pydantic import BaseModel, Field

def generate_event_id() -> str:
    """Generate unique UUID4 string for every event."""
    return str(uuid.uuid4())

def current_utc_iso() -> str:
    """Return current timestamp in ISO 8601 UTC format."""
    return datetime.now(timezone.utc).isoformat()

# =====================================================
# SPECIALIZED PAYLOAD SCHEMAS
# =====================================================

class VehicleTelemetryPayload(BaseModel):
    """Payload schema for vehicle GPS telemetry stream."""
    vehicle_id: str
    latitude: float = Field(..., ge=-90.0, le=90.0)
    longitude: float = Field(..., ge=-180.0, le=180.0)
    speed: float = Field(..., ge=0.0)
    heading: float = Field(default=0.0, ge=0.0, le=360.0, description="Direction in degrees (0-360)")
    vehicle_status: str = Field(default="MOVING", description="Current status: MOVING, STOPPED, DELAYED")
    timestamp: str = Field(default_factory=current_utc_iso)

class VehicleStatusPayload(BaseModel):
    """Payload schema for vehicle state transition events."""
    vehicle_id: str
    previous_status: str
    new_status: str
    reason: Optional[str] = "Normal operational transition"
    speed: Optional[float] = 0.0
    timestamp: str = Field(default_factory=current_utc_iso)

class OrderEventPayload(BaseModel):
    """Payload schema for order lifecycle events."""
    order_id: str
    event_action: Literal[
        "CREATED", "ASSIGNED", "PICKED_UP", "IN_TRANSIT", "DELIVERED", "REASSIGNED"
    ]
    assigned_vehicle_id: Optional[str] = None
    priority: Optional[str] = "MEDIUM"
    pickup_latitude: float = Field(..., ge=-90.0, le=90.0)
    pickup_longitude: float = Field(..., ge=-180.0, le=180.0)
    destination_latitude: float = Field(..., ge=-90.0, le=90.0)
    destination_longitude: float = Field(..., ge=-180.0, le=180.0)
    order_status: str
    delivery_deadline: Optional[str] = None
    estimated_delivery_time: Optional[str] = None
    actual_delivery_time: Optional[str] = None
    timestamp: str = Field(default_factory=current_utc_iso)

class DisruptionEventPayload(BaseModel):
    """Payload schema for confirmed stoppage or breakdown events."""
    disruption_id: str = Field(default_factory=lambda: f"DIS-{uuid.uuid4().hex[:8].upper()}")
    vehicle_id: str
    disruption_type: Literal["STOPPAGE", "BREAKDOWN", "TRAFFIC_CONGESTION"] = "STOPPAGE"
    severity: Literal["LOW", "MEDIUM", "CRITICAL"] = "MEDIUM"
    latitude: float = Field(..., ge=-90.0, le=90.0)
    longitude: float = Field(..., ge=-180.0, le=180.0)
    stoppage_duration_seconds: float = Field(..., ge=0.0)
    affected_order_ids: List[str] = Field(default_factory=list)
    timestamp: str = Field(default_factory=current_utc_iso)

class RecoveryEventPayload(BaseModel):
    """Payload schema for dynamic fleet recovery recommendations."""
    recovery_id: str = Field(default_factory=lambda: f"REC-{uuid.uuid4().hex[:8].upper()}")
    disrupted_vehicle_id: str
    candidate_vehicle_id: str
    order_id: Optional[str] = None
    reassigned_order_ids: List[str] = Field(default_factory=list)
    distance_km: float = Field(..., ge=0.0)
    status: str = "RECOMMENDED"
    timestamp: str = Field(default_factory=current_utc_iso)

# =====================================================
# UNIFIED ENVELOPE SCHEMA (VERSION 1.0)
# =====================================================

class StreamEventEnvelope(BaseModel):
    """
    Standardized CloudEvents-compliant Event Envelope for LastMile AI.
    Every event streaming through Condense / Kafka uses this schema.
    """
    event_id: str = Field(default_factory=generate_event_id)
    event_type: str = Field(..., description="E.g. vehicle_telemetry, order_events, disruption_events")
    timestamp: str = Field(default_factory=current_utc_iso)
    entity_id: str = Field(..., description="ID of entity: vehicle_id, order_id, or disruption_id")
    schema_version: str = Field(default="1.0", description="Event schema version")
    event_payload: Dict[str, Any] = Field(..., description="Structured payload matching the event_type")

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    @classmethod
    def create(cls, event_type: str, entity_id: str, payload: BaseModel | Dict[str, Any]) -> "StreamEventEnvelope":
        payload_dict = payload.model_dump() if isinstance(payload, BaseModel) else payload
        return cls(
            event_id=generate_event_id(),
            event_type=event_type,
            timestamp=current_utc_iso(),
            entity_id=entity_id,
            schema_version="1.0",
            event_payload=payload_dict
        )

# Mapping of topic to payload model for strict runtime validation
TOPIC_PAYLOAD_MODELS = {
    "vehicle_telemetry": VehicleTelemetryPayload,
    "vehicle_status": VehicleStatusPayload,
    "order_events": OrderEventPayload,
    "disruption_events": DisruptionEventPayload,
    "recovery_events": RecoveryEventPayload,
}

def validate_stream_event(data: Dict[str, Any]) -> StreamEventEnvelope:
    """Validate incoming dict against envelope and specialized payload schema."""
    envelope = StreamEventEnvelope.model_validate(data)
    expected_model = TOPIC_PAYLOAD_MODELS.get(envelope.event_type)
    if expected_model:
        # Validate inner payload matches topic schema
        expected_model.model_validate(envelope.event_payload)
    return envelope
