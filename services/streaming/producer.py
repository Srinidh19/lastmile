import json
import logging
import time
from typing import Dict, Any, List, Optional

from services.streaming.config import config
from services.streaming.schemas import (
    StreamEventEnvelope,
    VehicleTelemetryPayload,
    VehicleStatusPayload,
    OrderEventPayload,
    DisruptionEventPayload,
    RecoveryEventPayload,
    validate_stream_event
)
from services.streaming.broker import broker_manager

logger = logging.getLogger("LastMile.Streaming.Producer")

class EventProducer:
    """
    High-level Kafka / Event Stream Producer for LastMile AI.
    Formats, validates, and publishes structured events to specific Kafka topics.
    """
    def __init__(self, broker=broker_manager):
        self.broker = broker
        self.published_counts: Dict[str, int] = {
            config.topic_vehicle_telemetry: 0,
            config.topic_vehicle_status: 0,
            config.topic_order_events: 0,
            config.topic_disruption_events: 0,
            config.topic_recovery_events: 0,
        }

    def publish(self, topic: str, envelope: StreamEventEnvelope) -> bool:
        """
        Publish validated StreamEventEnvelope to topic with retry logic.
        """
        key = envelope.entity_id
        event_dict = envelope.to_dict()
        payload_json = json.dumps(event_dict)

        max_attempts = config.max_retries
        for attempt in range(1, max_attempts + 1):
            try:
                success = self.broker.publish_raw(topic, key, payload_json, event_dict)
                if success:
                    self.published_counts[topic] = self.published_counts.get(topic, 0) + 1
                    logger.debug(f"Published event {envelope.event_id} ({envelope.event_type}) to '{topic}'")
                    return True
            except Exception as e:
                logger.warning(
                    f"Attempt {attempt}/{max_attempts} failed to publish to '{topic}': {e}"
                )
                if attempt < max_attempts:
                    time.sleep((config.retry_backoff_ms / 1000.0) * (2 ** (attempt - 1)))
        
        logger.error(f"Failed to publish event {envelope.event_id} to '{topic}' after {max_attempts} attempts.")
        return False

    def publish_vehicle_telemetry(
        self,
        vehicle_id: str,
        latitude: float,
        longitude: float,
        speed: float,
        heading: float = 0.0,
        vehicle_status: str = "MOVING"
    ) -> StreamEventEnvelope:
        """Publish GPS telemetry update."""
        payload = VehicleTelemetryPayload(
            vehicle_id=vehicle_id,
            latitude=latitude,
            longitude=longitude,
            speed=speed,
            heading=heading,
            vehicle_status=vehicle_status
        )
        envelope = StreamEventEnvelope.create(
            event_type=config.topic_vehicle_telemetry,
            entity_id=vehicle_id,
            payload=payload
        )
        self.publish(config.topic_vehicle_telemetry, envelope)
        return envelope

    def publish_vehicle_status(
        self,
        vehicle_id: str,
        previous_status: str,
        new_status: str,
        reason: str = "Status transition",
        speed: float = 0.0
    ) -> StreamEventEnvelope:
        """Publish vehicle state transition event."""
        payload = VehicleStatusPayload(
            vehicle_id=vehicle_id,
            previous_status=previous_status,
            new_status=new_status,
            reason=reason,
            speed=speed
        )
        envelope = StreamEventEnvelope.create(
            event_type=config.topic_vehicle_status,
            entity_id=vehicle_id,
            payload=payload
        )
        self.publish(config.topic_vehicle_status, envelope)
        return envelope

    def publish_order_event(
        self,
        order_id: str,
        event_action: str,
        pickup_latitude: float,
        pickup_longitude: float,
        destination_latitude: float,
        destination_longitude: float,
        order_status: str,
        assigned_vehicle_id: Optional[str] = None,
        priority: str = "MEDIUM",
        delivery_deadline: Optional[str] = None,
        estimated_delivery_time: Optional[str] = None,
        actual_delivery_time: Optional[str] = None
    ) -> StreamEventEnvelope:
        """Publish order lifecycle progression event."""
        payload = OrderEventPayload(
            order_id=order_id,
            event_action=event_action,  # type: ignore
            assigned_vehicle_id=assigned_vehicle_id,
            priority=priority,
            pickup_latitude=pickup_latitude,
            pickup_longitude=pickup_longitude,
            destination_latitude=destination_latitude,
            destination_longitude=destination_longitude,
            order_status=order_status,
            delivery_deadline=delivery_deadline,
            estimated_delivery_time=estimated_delivery_time,
            actual_delivery_time=actual_delivery_time
        )
        envelope = StreamEventEnvelope.create(
            event_type=config.topic_order_events,
            entity_id=order_id,
            payload=payload
        )
        self.publish(config.topic_order_events, envelope)
        return envelope

    def publish_disruption_event(
        self,
        vehicle_id: str,
        disruption_type: str,
        severity: str,
        latitude: float,
        longitude: float,
        stoppage_duration_seconds: float,
        affected_order_ids: Optional[List[str]] = None
    ) -> StreamEventEnvelope:
        """Publish vehicle disruption / confirmed stoppage alert."""
        payload = DisruptionEventPayload(
            vehicle_id=vehicle_id,
            disruption_type=disruption_type,  # type: ignore
            severity=severity,  # type: ignore
            latitude=latitude,
            longitude=longitude,
            stoppage_duration_seconds=stoppage_duration_seconds,
            affected_order_ids=affected_order_ids or []
        )
        envelope = StreamEventEnvelope.create(
            event_type=config.topic_disruption_events,
            entity_id=vehicle_id,
            payload=payload
        )
        self.publish(config.topic_disruption_events, envelope)
        return envelope

    def publish_recovery_event(
        self,
        disrupted_vehicle_id: str,
        candidate_vehicle_id: str,
        distance_km: float,
        order_id: Optional[str] = None,
        reassigned_order_ids: Optional[List[str]] = None,
        status: str = "RECOMMENDED"
    ) -> StreamEventEnvelope:
        """Publish fleet recovery recommendation event."""
        payload = RecoveryEventPayload(
            disrupted_vehicle_id=disrupted_vehicle_id,
            candidate_vehicle_id=candidate_vehicle_id,
            order_id=order_id,
            reassigned_order_ids=reassigned_order_ids or ([order_id] if order_id else []),
            distance_km=distance_km,
            status=status
        )
        envelope = StreamEventEnvelope.create(
            event_type=config.topic_recovery_events,
            entity_id=disrupted_vehicle_id,
            payload=payload
        )
        self.publish(config.topic_recovery_events, envelope)
        return envelope

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_published": sum(self.published_counts.values()),
            "published_by_topic": dict(self.published_counts)
        }

producer = EventProducer()
