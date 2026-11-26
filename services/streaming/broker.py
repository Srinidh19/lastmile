import json
import logging
import queue
import threading
import time
from typing import Dict, Any, List, Optional, Callable

from services.streaming.config import StreamingConfig, config

logger = logging.getLogger("LastMile.Streaming")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] [Streaming] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Optional Kafka client imports
CONFLUENT_AVAILABLE = False
try:
    import confluent_kafka
    CONFLUENT_AVAILABLE = True
except ImportError:
    pass

KAFKA_PYTHON_AVAILABLE = False
try:
    from kafka import KafkaProducer as KPProducer, KafkaConsumer as KPConsumer
    KAFKA_PYTHON_AVAILABLE = True
except ImportError:
    pass


class InMemoryTopicBus:
    """
    High-performance, thread-safe in-memory pub-sub message broker.
    Provides identical semantics to Kafka topics for reliable local execution,
    unit testing, and zero-dependency fallbacks.
    """
    def __init__(self):
        self._lock = threading.RLock()
        self._topics: Dict[str, List[queue.Queue]] = {}
        self._history: List[Dict[str, Any]] = []
        self._max_history = 1000

    def register_consumer(self, topic: str) -> queue.Queue:
        with self._lock:
            if topic not in self._topics:
                self._topics[topic] = []
            q: queue.Queue = queue.Queue(maxsize=10000)
            self._topics[topic].append(q)
            return q

    def unregister_consumer(self, topic: str, q: queue.Queue):
        with self._lock:
            if topic in self._topics and q in self._topics[topic]:
                self._topics[topic].remove(q)

    def publish(self, topic: str, event_data: Dict[str, Any]):
        with self._lock:
            self._history.append({"topic": topic, "event": event_data, "time": time.time()})
            if len(self._history) > self._max_history:
                self._history.pop(0)

            subscribers = self._topics.get(topic, [])
            for q in subscribers:
                try:
                    q.put_nowait(event_data)
                except queue.Full:
                    logger.warning(f"Consumer queue full for topic '{topic}', dropping oldest event")
                    try:
                        q.get_nowait()
                        q.put_nowait(event_data)
                    except Exception:
                        pass

    def get_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._history[-limit:])


# Global in-memory bus singleton
in_memory_bus = InMemoryTopicBus()


class StreamingBrokerManager:
    """
    Manages connection to Condense / Apache Kafka cluster or gracefully
    falls back to the in-memory bus when Kafka is unreachable.
    """
    def __init__(self, cfg: Optional[StreamingConfig] = None):
        self.cfg = cfg or config
        self.mode = "IN_MEMORY"
        self.is_kafka_connected = False
        self._ck_producer = None
        self._probe_kafka()

    def _probe_kafka(self):
        """Test if configured Kafka / Condense broker is reachable."""
        if self.cfg.streaming_mode == "inmemory":
            logger.info("Streaming mode set to 'inmemory'. Using local pub/sub engine.")
            self.mode = "IN_MEMORY"
            self.is_kafka_connected = False
            return

        if CONFLUENT_AVAILABLE:
            try:
                test_conf = self.cfg.get_kafka_producer_config()
                # Test connectivity with short timeout
                test_conf["socket.timeout.ms"] = min(3000, self.cfg.connection_timeout_ms)
                producer = confluent_kafka.Producer(test_conf)
                # Attempt metadata lookup to verify cluster reachability
                cluster_metadata = producer.list_topics(timeout=2.0)
                if cluster_metadata and cluster_metadata.brokers:
                    self._ck_producer = producer
                    self.is_kafka_connected = True
                    self.mode = "CONFLUENT_KAFKA"
                    logger.info(
                        f"Connected to Kafka / Condense backbone at {self.cfg.bootstrap_servers} "
                        f"(Brokers: {len(cluster_metadata.brokers)})"
                    )
                    return
            except Exception as e:
                logger.warning(
                    f"Live Kafka cluster at '{self.cfg.bootstrap_servers}' not reachable ({e}). "
                    f"Seamlessly activating robust in-memory streaming bus."
                )

        if not self.is_kafka_connected:
            self.mode = "IN_MEMORY"
            logger.info("Using active in-memory streaming backbone for telemetry and events.")

    def publish_raw(self, topic: str, key: str, payload_json: str, event_dict: Dict[str, Any]) -> bool:
        """Publish message to Kafka topic or in-memory bus."""
        # Always mirror to in-memory bus for local observability / event inspection
        in_memory_bus.publish(topic, event_dict)

        if self.is_kafka_connected and self._ck_producer:
            try:
                self._ck_producer.produce(
                    topic=topic,
                    key=key.encode("utf-8") if key else None,
                    value=payload_json.encode("utf-8"),
                    on_delivery=self._delivery_report
                )
                self._ck_producer.poll(0)
                return True
            except Exception as e:
                logger.error(f"Failed publishing to Kafka topic '{topic}': {e}. Event safely in memory.")
                return False
        return True

    def _delivery_report(self, err, msg):
        if err:
            logger.error(f"Kafka message delivery failed: {err}")
        else:
            logger.debug(f"Delivered message to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def flush(self, timeout: float = 2.0):
        if self.is_kafka_connected and self._ck_producer:
            try:
                self._ck_producer.flush(timeout)
            except Exception:
                pass

    def get_status(self) -> Dict[str, Any]:
        return {
            "mode": self.mode,
            "is_kafka_connected": self.is_kafka_connected,
            "bootstrap_servers": self.cfg.bootstrap_servers,
            "security_protocol": self.cfg.security_protocol,
            "sasl_mechanism": self.cfg.sasl_mechanism if self.cfg.security_protocol != "PLAINTEXT" else "NONE",
            "topics": [
                self.cfg.topic_vehicle_telemetry,
                self.cfg.topic_vehicle_status,
                self.cfg.topic_order_events,
                self.cfg.topic_disruption_events,
                self.cfg.topic_recovery_events,
            ],
            "consumer_group": self.cfg.consumer_group
        }

broker_manager = StreamingBrokerManager()
