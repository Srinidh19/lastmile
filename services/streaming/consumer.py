import json
import logging
import queue
import threading
import time
from collections import OrderedDict
from typing import Dict, Any, List, Optional, Callable

from services.streaming.config import config
from services.streaming.schemas import StreamEventEnvelope, validate_stream_event
from services.streaming.broker import broker_manager, in_memory_bus, CONFLUENT_AVAILABLE

logger = logging.getLogger("LastMile.Streaming.Consumer")

class EventDeduplicator:
    """
    In-memory bounded LRU cache for duplicate event protection.
    Prevents re-processing identical event IDs across consumers.
    """
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self._lock = threading.Lock()
        self._cache = OrderedDict()

    def is_duplicate(self, event_id: str) -> bool:
        with self._lock:
            if event_id in self._cache:
                return True
            self._cache[event_id] = time.time()
            if len(self._cache) > self.max_size:
                self._cache.popitem(last=False)
            return False

class EventConsumer:
    """
    Multi-topic Kafka & Streaming Consumer for LastMile AI.
    Runs continuous background worker loop, validates schemas, eliminates duplicates,
    and dispatches events to registered application handlers.
    """
    def __init__(self, group_id: Optional[str] = None):
        self.group_id = group_id or config.consumer_group
        self.handlers: Dict[str, List[Callable[[StreamEventEnvelope], None]]] = {}
        self.deduplicator = EventDeduplicator()
        self.is_running = False
        self._threads: List[threading.Thread] = []
        self._in_memory_queues: Dict[str, queue.Queue] = {}
        self._ck_consumer = None
        self.consumed_counts: Dict[str, int] = {}
        self.duplicate_counts: Dict[str, int] = {}
        self.validation_error_counts: Dict[str, int] = {}

    def register_handler(self, topic: str, handler: Callable[[StreamEventEnvelope], None]):
        """Register a callback handler for a specific topic."""
        if topic not in self.handlers:
            self.handlers[topic] = []
        self.handlers[topic].append(handler)
        logger.info(f"Registered handler for topic '{topic}': {handler.__name__}")

    def start(self):
        """Start the background consumer threads."""
        if self.is_running:
            return
        self.is_running = True

        all_topics = [
            config.topic_vehicle_telemetry,
            config.topic_vehicle_status,
            config.topic_order_events,
            config.topic_disruption_events,
            config.topic_recovery_events,
        ]

        for topic in all_topics:
            self.consumed_counts[topic] = 0
            self.duplicate_counts[topic] = 0
            self.validation_error_counts[topic] = 0

        # If live Kafka is connected, start Kafka consumer worker
        if broker_manager.is_kafka_connected and CONFLUENT_AVAILABLE:
            t = threading.Thread(target=self._kafka_worker_loop, args=(all_topics,), daemon=True)
            t.name = "KafkaConsumerThread"
            t.start()
            self._threads.append(t)
            logger.info(f"Started Confluent Kafka consumer thread for group '{self.group_id}'")

        # Always start in-memory worker for real-time local responsiveness and failover
        t_mem = threading.Thread(target=self._in_memory_worker_loop, args=(all_topics,), daemon=True)
        t_mem.name = "InMemoryStreamingThread"
        t_mem.start()
        self._threads.append(t_mem)
        logger.info("Started In-Memory streaming dispatch thread.")

    def stop(self):
        """Signal consumer to stop gracefully."""
        self.is_running = False
        if self._ck_consumer:
            try:
                self._ck_consumer.close()
            except Exception:
                pass
        for topic, q in self._in_memory_queues.items():
            in_memory_bus.unregister_consumer(topic, q)

    def _dispatch_raw(self, topic: str, event_data: Dict[str, Any]):
        """Validate, deduplicate, and dispatch an event."""
        # 1. Schema Validation
        try:
            envelope = validate_stream_event(event_data)
        except Exception as e:
            self.validation_error_counts[topic] = self.validation_error_counts.get(topic, 0) + 1
            logger.error(f"Event schema validation failed on topic '{topic}': {e}. Payload: {event_data}")
            return

        # 2. Duplicate Event Protection
        if self.deduplicator.is_duplicate(envelope.event_id):
            self.duplicate_counts[topic] = self.duplicate_counts.get(topic, 0) + 1
            logger.debug(f"Duplicate event suppressed: {envelope.event_id} on '{topic}'")
            return

        # 3. Dispatch to registered handlers
        self.consumed_counts[topic] = self.consumed_counts.get(topic, 0) + 1
        callbacks = self.handlers.get(topic, [])
        for cb in callbacks:
            try:
                cb(envelope)
            except Exception as e:
                logger.error(f"Error in consumer callback '{cb.__name__}' for topic '{topic}': {e}", exc_info=True)

    def _in_memory_worker_loop(self, topics: List[str]):
        """Polls in-memory bus queues for events."""
        for topic in topics:
            self._in_memory_queues[topic] = in_memory_bus.register_consumer(topic)

        while self.is_running:
            did_work = False
            for topic in topics:
                q = self._in_memory_queues.get(topic)
                if not q:
                    continue
                try:
                    event_data = q.get_nowait()
                    did_work = True
                    self._dispatch_raw(topic, event_data)
                except queue.Empty:
                    pass
                except Exception as e:
                    logger.error(f"In-memory worker error on topic '{topic}': {e}")

            if not did_work:
                time.sleep(0.05)  # 50ms tick to yield CPU

    def _kafka_worker_loop(self, topics: List[str]):
        """Polls Confluent Kafka consumer if connected."""
        import confluent_kafka
        consumer_conf = config.get_kafka_consumer_config(self.group_id)
        consumer = confluent_kafka.Consumer(consumer_conf)
        self._ck_consumer = consumer
        consumer.subscribe(topics)

        while self.is_running:
            try:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.warning(f"Kafka consumer error: {msg.error()}")
                    continue

                topic = msg.topic()
                val_str = msg.value().decode("utf-8")
                event_dict = json.loads(val_str)
                self._dispatch_raw(topic, event_dict)
            except Exception as e:
                if self.is_running:
                    logger.error(f"Kafka consumer poll exception: {e}")
                    time.sleep(1.0)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "is_running": self.is_running,
            "consumer_group": self.group_id,
            "total_consumed": sum(self.consumed_counts.values()),
            "consumed_by_topic": dict(self.consumed_counts),
            "duplicates_filtered": dict(self.duplicate_counts),
            "validation_errors": dict(self.validation_error_counts),
        }

consumer = EventConsumer()
