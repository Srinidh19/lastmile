"""
LastMile AI - Condense + Apache Kafka Real-Time Event Streaming Backbone
Provides structured, validated, and resilient streaming for:
- vehicle_telemetry
- vehicle_status
- order_events
- disruption_events
- recovery_events
"""

from services.streaming.config import StreamingConfig, config
from services.streaming.schemas import (
    StreamEventEnvelope,
    VehicleTelemetryPayload,
    VehicleStatusPayload,
    OrderEventPayload,
    DisruptionEventPayload,
    RecoveryEventPayload,
    validate_stream_event,
)
from services.streaming.producer import EventProducer, producer
from services.streaming.consumer import EventConsumer, consumer
from services.streaming.manager import StreamingManager, streaming_manager

__all__ = [
    "StreamingConfig",
    "config",
    "StreamEventEnvelope",
    "VehicleTelemetryPayload",
    "VehicleStatusPayload",
    "OrderEventPayload",
    "DisruptionEventPayload",
    "RecoveryEventPayload",
    "validate_stream_event",
    "EventProducer",
    "producer",
    "EventConsumer",
    "consumer",
    "StreamingManager",
    "streaming_manager",
]
