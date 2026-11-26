import logging
from typing import Dict, Any, List, Optional, Callable

from services.streaming.config import config
from services.streaming.schemas import StreamEventEnvelope
from services.streaming.broker import broker_manager, in_memory_bus
from services.streaming.producer import producer, EventProducer
from services.streaming.consumer import consumer, EventConsumer

logger = logging.getLogger("LastMile.Streaming.Manager")

class StreamingManager:
    """
    Central orchestration facade for LastMile AI Real-Time Streaming Backbone.
    Coordinates topics, producers, consumers, and status reporting.
    """
    def __init__(self):
        self.config = config
        self.broker = broker_manager
        self.producer = producer
        self.consumer = consumer

    def start(self):
        """Start the streaming engine and consumers."""
        logger.info("Initializing Real-Time Streaming Backbone (Condense / Kafka)...")
        self.consumer.start()
        logger.info("Streaming Backbone active.")

    def stop(self):
        """Stop consumers and flush pending producer messages."""
        logger.info("Shutting down Streaming Backbone...")
        self.consumer.stop()
        self.broker.flush(2.0)
        logger.info("Streaming Backbone stopped.")

    def on_event(self, topic: str):
        """Decorator for registering topic callbacks."""
        def decorator(fn: Callable[[StreamEventEnvelope], None]):
            self.consumer.register_handler(topic, fn)
            return fn
        return decorator

    def get_overview(self) -> Dict[str, Any]:
        """Comprehensive telemetry and diagnostic overview for APIs and Dashboard."""
        broker_status = self.broker.get_status()
        prod_stats = self.producer.get_stats()
        cons_stats = self.consumer.get_stats()
        recent = in_memory_bus.get_history(limit=20)

        return {
            "status": "HEALTHY",
            "streaming_mode": broker_status["mode"],
            "kafka_connected": broker_status["is_kafka_connected"],
            "bootstrap_servers": broker_status["bootstrap_servers"],
            "security_protocol": broker_status["security_protocol"],
            "consumer_group": broker_status["consumer_group"],
            "active_topics": broker_status["topics"],
            "producer_stats": prod_stats,
            "consumer_stats": cons_stats,
            "recent_events": [
                {
                    "topic": item["topic"],
                    "event_id": item["event"].get("event_id"),
                    "event_type": item["event"].get("event_type"),
                    "entity_id": item["event"].get("entity_id"),
                    "timestamp": item["event"].get("timestamp")
                }
                for item in reversed(recent)
            ]
        }

streaming_manager = StreamingManager()
