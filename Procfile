web: uvicorn backend.main:app --host 0.0.0.0 --port ${PORT:-8000}
worker: python simulator/vehicle_simulator.py
frontend: streamlit run frontend/app.py --server.port ${PORT:-8501} --server.address 0.0.0.0
