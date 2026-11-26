# LastMile AI — Mobility & Order Management Intelligence Platform

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-v0.115-green.svg)](https://fastapi.tiangolo.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-v1.35-red.svg)](https://streamlit.io/)
[![Streaming](https://img.shields.io/badge/Streaming-Condense%20%2B%20Kafka-purple.svg)](https://zeliot.in/)
[![Routing](https://img.shields.io/badge/Routing-OSRM%20Road%20Engine-orange.svg)](https://project-osrm.org/)

LastMile AI is a real-time logistics intelligence platform designed to monitor delivery fleets, track order lifecycles, detect vehicle stagnations and disruptions, predict delivery delays, and recommend recovery actions.

---

## Key Features

- **Real-Time Fleet Telemetry**: 5 simulated vehicles streaming continuous GPS coordinates, speed, heading, and status across Delhi NCR.
- **Order Lifecycle Engine**: Complete order states (`PENDING` $\rightarrow$ `ASSIGNED` $\rightarrow$ `PICKED_UP` $\rightarrow$ `IN_TRANSIT` $\rightarrow$ `DELIVERED` / `DELAYED`), respecting vehicle capacity (max 10 orders/vehicle).
- **OSRM Road Routing**: Real road-following routes via OSRM with coordinates ordering, distance calculations, and smart caching.
- **Stagnation & Disruption Detection**: Real-time detection of vehicle stoppages ($>10\text{s}$ medium, $\ge 20\text{s}$ critical) triggering automated disruption alerts and recovery candidate ranking.
- **Condense + Apache Kafka Streaming Backbone**: Pub-sub architecture with topics `vehicle_telemetry`, `vehicle_status`, `order_events`, `disruption_events`, and `recovery_events` featuring LRU deduplication and CloudEvents schemas.
- **Control Tower Dashboard**: Interactive Streamlit frontend with Folium maps, live telemetry metrics, vehicle capacity monitors, and dispatch registry.

---

## Project Structure

```
├── backend/
│   └── main.py                     # FastAPI REST API & Kafka stream consumer
├── frontend/
│   └── app.py                      # Streamlit Control Tower Dashboard
├── services/
│   ├── order_service.py            # Order management engine & lifecycle state machine
│   ├── routing_service.py          # OSRM road routing engine & Haversine fallback
│   └── streaming/                  # Condense + Kafka real-time streaming backbone
│       ├── broker.py               # Dual-transport Kafka / In-Memory manager
│       ├── config.py               # Kafka / Condense credentials and settings
│       ├── consumer.py             # Multi-topic consumer with deduplication
│       ├── manager.py              # Central streaming orchestrator
│       ├── producer.py             # EventProducer with retry mechanism
│       └── schemas.py              # CloudEvents envelopes & payload validation
├── simulator/
│   └── vehicle_simulator.py        # 5-vehicle telemetry generator & Kafka producer
├── tests/
│   ├── test_audit_verification.py  # 13 audit verification tests
│   ├── test_streaming.py           # 7 end-to-end event flow tests
│   └── test_system.py              # Integration tests
├── .streamlit/
│   └── config.toml                 # Streamlit theme & server configuration
├── render.yaml                     # Render 1-click blueprint (FastAPI + Worker)
├── Procfile                        # PaaS process file (Railway / Render / Heroku)
├── Dockerfile                      # Container specification
├── docker-compose.yml              # Local multi-service orchestration
└── requirements.txt                # Python dependencies
```

---

## Quickstart

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Start FastAPI Backend
```bash
python -m uvicorn backend.main:app --host 127.0.0.1 --port 8000
```
- API Docs: `http://127.0.0.1:8000/docs`
- Streaming Status: `http://127.0.0.1:8000/streaming/status`

### 3. Start Vehicle Simulator
```bash
python simulator/vehicle_simulator.py
```

### 4. Launch Streamlit Control Tower
```bash
python -m streamlit run frontend/app.py --server.port 8501
```
- Dashboard: `http://localhost:8501`
