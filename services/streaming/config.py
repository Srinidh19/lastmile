import os
from typing import Optional
from pydantic import BaseModel, Field

class StreamingConfig(BaseModel):
    """
    Configuration for Condense and Apache Kafka Event Streaming Backbone.
    Supports environment variables with sensible defaults for local and cloud environments.
    """
    # Kafka / Condense Cluster Brokers
    bootstrap_servers: str = Field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    
    # Security Protocol: PLAINTEXT, SASL_SSL, SASL_PLAINTEXT, SSL
    security_protocol: str = Field(
        default_factory=lambda: os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
    )
    
    # SASL Mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
    sasl_mechanism: str = Field(
        default_factory=lambda: os.getenv("KAFKA_SASL_MECHANISM", "PLAIN").upper()
    )
    
    # Credentials (Condense API Key / Secret)
    sasl_username: Optional[str] = Field(
        default_factory=lambda: os.getenv("KAFKA_SASL_USERNAME") or os.getenv("CONDENSE_API_KEY")
    )
    sasl_password: Optional[str] = Field(
        default_factory=lambda: os.getenv("KAFKA_SASL_PASSWORD") or os.getenv("CONDENSE_API_SECRET")
    )
    
    # Client and Consumer Group IDs
    client_id: str = Field(
        default_factory=lambda: os.getenv("KAFKA_CLIENT_ID", "lastmile-ai-node")
    )
    consumer_group: str = Field(
        default_factory=lambda: os.getenv("KAFKA_CONSUMER_GROUP", "lastmile-backend-group")
    )
    auto_offset_reset: str = Field(
        default_factory=lambda: os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
    )
    
    # Topics
    topic_vehicle_telemetry: str = "vehicle_telemetry"
    topic_vehicle_status: str = "vehicle_status"
    topic_order_events: str = "order_events"
    topic_disruption_events: str = "disruption_events"
    topic_recovery_events: str = "recovery_events"
    
    # Operational Settings
    connection_timeout_ms: int = 5000
    request_timeout_ms: int = 10000
    retry_backoff_ms: int = 500
    max_retries: int = 3
    
    # Streaming Mode: 'auto' (tries Kafka broker first, falls back to memory if unreachable),
    # 'kafka' (strict Kafka only), 'inmemory' (for isolated testing)
    streaming_mode: str = Field(
        default_factory=lambda: os.getenv("STREAMING_MODE", "auto").lower()
    )

    def get_kafka_producer_config(self) -> dict:
        """Generate Confluent/Kafka compatible client configuration dictionary."""
        cfg = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": f"{self.client_id}-producer",
            "retries": self.max_retries,
            "retry.backoff.ms": self.retry_backoff_ms,
            "socket.timeout.ms": self.connection_timeout_ms,
            "request.timeout.ms": self.request_timeout_ms,
            "acks": "all",
        }
        if self.security_protocol in ["SASL_SSL", "SASL_PLAINTEXT"]:
            cfg["security.protocol"] = self.security_protocol
            cfg["sasl.mechanism"] = self.sasl_mechanism
            if self.sasl_username:
                cfg["sasl.username"] = self.sasl_username
            if self.sasl_password:
                cfg["sasl.password"] = self.sasl_password
        elif self.security_protocol == "SSL":
            cfg["security.protocol"] = "SSL"

        return cfg

    def get_kafka_consumer_config(self, group_id: Optional[str] = None) -> dict:
        """Generate Confluent/Kafka consumer configuration dictionary."""
        cfg = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id or self.consumer_group,
            "client.id": f"{self.client_id}-consumer",
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": True,
            "socket.timeout.ms": self.connection_timeout_ms,
        }
        if self.security_protocol in ["SASL_SSL", "SASL_PLAINTEXT"]:
            cfg["security.protocol"] = self.security_protocol
            cfg["sasl.mechanism"] = self.sasl_mechanism
            if self.sasl_username:
                cfg["sasl.username"] = self.sasl_username
            if self.sasl_password:
                cfg["sasl.password"] = self.sasl_password
        elif self.security_protocol == "SSL":
            cfg["security.protocol"] = "SSL"

        return cfg

# Global singleton configuration
config = StreamingConfig()
