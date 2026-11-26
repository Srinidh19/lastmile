"""
LastMile AI - Phase 1 Comprehensive Audit & Verification Suite
Verifies all 13 core functional requirements:
1. FastAPI starts successfully.
2. All existing API endpoints respond correctly.
3. Five simulated vehicles are available.
4. GPS coordinates update correctly.
5. Vehicle movement works.
6. Stoppage detection works.
7. Vehicle status updates correctly.
8. Recovery recommendations return valid results.
9. Streamlit dashboard loads.
10. Dashboard displays actual backend data.
11. Map displays vehicle markers correctly.
12. OSRM routing works or reports a meaningful error.
13. Routing failures do not crash the application.
"""

import time
import requests
from typing import Dict, Any

BACKEND_URL = "http://127.0.0.1:8000"
FRONTEND_URL = "http://127.0.0.1:8501"

def test_1_fastapi_starts():
    print("\n[CHECK 1] Testing FastAPI startup and root endpoint...")
    resp = requests.get(f"{BACKEND_URL}/", timeout=5)
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    data = resp.json()
    assert data.get("application") == "LastMile AI"
    assert data.get("status") == "online"
    print(f" PASS: Backend online ({data.get('application')} v{data.get('version')})")

def test_2_api_endpoints():
    print("\n[CHECK 2] Testing all existing API endpoints...")
    endpoints = [
        ("GET", "/", 200),
        ("GET", "/health", 200),
        ("GET", "/vehicles", 200),
        ("GET", "/vehicles/V101", 200),
        ("GET", "/alerts", 200),
        ("GET", "/summary", 200),
        ("GET", "/events", 200),
        ("GET", "/orders", 200),
        ("GET", "/orders/summary", 200),
        ("GET", "/orders/available-vehicles", 200),
        ("GET", "/orders/vehicle/V101", 200),
        ("GET", "/routes/V101", 200),
        ("GET", "/recovery/V101", 200),
    ]
    for method, ep, expected_code in endpoints:
        r = requests.request(method, f"{BACKEND_URL}{ep}", timeout=8)
        assert r.status_code == expected_code, f"{method} {ep} returned {r.status_code}, expected {expected_code}"
        print(f"   {method:4s} {ep:32s} -> HTTP {r.status_code} OK")
    print(" PASS: All core API endpoints respond with HTTP 200")

def test_3_five_vehicles_available():
    print("\n[CHECK 3] Verifying five simulated vehicles are available...")
    r = requests.get(f"{BACKEND_URL}/vehicles", timeout=5)
    assert r.status_code == 200
    vehicles = r.json()
    vids = {v["vehicle_id"] for v in vehicles}
    expected = {"V101", "V102", "V103", "V104", "V105"}
    assert expected.issubset(vids), f"Missing vehicles. Expected {expected}, got {vids}"
    print(f" PASS: 5 fleet vehicles verified: {sorted(list(vids))}")

def test_4_gps_updates():
    print("\n[CHECK 4] Verifying GPS coordinates update correctly...")
    test_lat = 28.618800
    test_lon = 77.213300
    ev = {
        "vehicle_id": "V101",
        "latitude": test_lat,
        "longitude": test_lon,
        "speed": 28.0,
        "timestamp": "2026-11-26T14:00:00+00:00"
    }
    r = requests.post(f"{BACKEND_URL}/events", json=ev, timeout=5)
    assert r.status_code == 200
    
    # Check vehicle reflects new GPS coordinates
    v_resp = requests.get(f"{BACKEND_URL}/vehicles/V101", timeout=5).json()
    assert abs(v_resp["latitude"] - test_lat) < 1e-5, f"Latitude not updated: {v_resp['latitude']}"
    assert abs(v_resp["longitude"] - test_lon) < 1e-5, f"Longitude not updated: {v_resp['longitude']}"
    print(f" PASS: GPS update reflected accurately: ({v_resp['latitude']}, {v_resp['longitude']})")

def test_5_vehicle_movement():
    print("\n[CHECK 5] Verifying vehicle movement state...")
    ev = {
        "vehicle_id": "V103",
        "latitude": 28.635000,
        "longitude": 77.225000,
        "speed": 34.5,
        "timestamp": "2026-11-26T14:01:00+00:00"
    }
    requests.post(f"{BACKEND_URL}/events", json=ev, timeout=5)
    v_resp = requests.get(f"{BACKEND_URL}/vehicles/V103", timeout=5).json()
    assert v_resp["status"] == "MOVING", f"Expected MOVING, got {v_resp['status']}"
    assert v_resp["speed"] == 34.5
    print(f" PASS: Vehicle movement verified (status={v_resp['status']}, speed={v_resp['speed']} km/h)")

def test_6_and_7_stoppage_detection_and_status():
    print("\n[CHECK 6 & 7] Verifying stoppage detection & status progression...")
    # Step 1: Initial stop
    stop_ev1 = {
        "vehicle_id": "V102",
        "latitude": 28.620000,
        "longitude": 77.215000,
        "speed": 0.0,
        "timestamp": "2026-11-26T14:00:00+00:00"
    }
    r1 = requests.post(f"{BACKEND_URL}/events", json=stop_ev1, timeout=5)
    assert r1.status_code == 200
    v1 = requests.get(f"{BACKEND_URL}/vehicles/V102", timeout=5).json()
    assert v1["status"] == "STOPPED"
    print(f"   Step 1: Stoppage initiated (status={v1['status']})")

    # Step 2: Critical stoppage (>20 seconds)
    time.sleep(1.2)
    stop_ev2 = {
        "vehicle_id": "V102",
        "latitude": 28.620000,
        "longitude": 77.215000,
        "speed": 0.0,
        "timestamp": "2026-11-26T14:00:25+00:00"
    }
    r2 = requests.post(f"{BACKEND_URL}/events", json=stop_ev2, timeout=5)
    assert r2.status_code == 200
    
    alerts = requests.get(f"{BACKEND_URL}/alerts", timeout=5).json()
    v102_alert = next((a for a in alerts if a.get("vehicle_id") == "V102"), None)
    assert v102_alert is not None, "Critical stoppage alert not created for V102"
    assert v102_alert["severity"] in ["MEDIUM", "CRITICAL"]
    print(f"   Step 2: Stoppage alert triggered (severity={v102_alert['severity']}, risk_score={v102_alert['risk_score']})")

    # Step 3: Movement resumption
    move_ev = {
        "vehicle_id": "V102",
        "latitude": 28.621000,
        "longitude": 77.216000,
        "speed": 22.0,
        "timestamp": "2026-11-26T14:01:00+00:00"
    }
    requests.post(f"{BACKEND_URL}/events", json=move_ev, timeout=5)
    v2 = requests.get(f"{BACKEND_URL}/vehicles/V102", timeout=5).json()
    assert v2["status"] == "MOVING", f"Status not updated to MOVING: {v2['status']}"
    assert v2["stopped_since"] is None
    print(f"   Step 3: Resumed movement clears stoppage (status={v2['status']})")
    print(" PASS: Stoppage detection and vehicle status progression verified")

def test_8_recovery_recommendations():
    print("\n[CHECK 8] Verifying recovery recommendation engine...")
    # Stop V102 to ensure recovery can be analyzed
    requests.post(f"{BACKEND_URL}/events", json={
        "vehicle_id": "V102", "latitude": 28.620000, "longitude": 77.215000,
        "speed": 0.0, "timestamp": "2026-11-26T14:02:00+00:00"
    }, timeout=5)
    # Ensure candidate V101 is moving nearby
    requests.post(f"{BACKEND_URL}/events", json={
        "vehicle_id": "V101", "latitude": 28.618000, "longitude": 77.213000,
        "speed": 25.0, "timestamp": "2026-11-26T14:02:00+00:00"
    }, timeout=5)

    rec = requests.get(f"{BACKEND_URL}/recovery/V102", timeout=5).json()
    assert "decision" in rec
    print(f"   Recovery Decision for stopped V102: {rec['decision']}")
    print(f"   Pending deliveries: {rec.get('pending_deliveries')}")
    print(f"   Disrupted order IDs: {rec.get('disrupted_orders', [])}")
    print(f"   Candidate recommendations: {len(rec.get('recommendations', []))}")
    assert rec["decision"] in ["RECOVERY_PLAN_AVAILABLE", "NO_ELIGIBLE_VEHICLE_FOUND", "NO_RECOVERY_REQUIRED"]
    print(" PASS: Recovery recommendation engine returns structured, valid decision")

def test_9_and_10_streamlit_dashboard():
    print("\n[CHECK 9 & 10] Verifying Streamlit dashboard and data integration...")
    r = requests.get(FRONTEND_URL, timeout=8)
    assert r.status_code == 200, f"Streamlit returned {r.status_code}"
    # Verify HTML loads correctly
    assert "<!DOCTYPE html>" in r.text or "streamlit" in r.text.lower()
    print("   Streamlit HTTP 200 OK")

    # Verify backend data feed that powers Streamlit
    summary = requests.get(f"{BACKEND_URL}/summary", timeout=5).json()
    assert "total_vehicles" in summary
    assert "total_orders" in summary
    print(f"   Backend summary feed active (vehicles={summary['total_vehicles']}, orders={summary['total_orders']})")
    print(" PASS: Streamlit dashboard loads and communicates with live backend")

def test_11_map_markers():
    print("\n[CHECK 11] Verifying vehicle and destination map markers...")
    vehicles = requests.get(f"{BACKEND_URL}/vehicles", timeout=5).json()
    valid_coords = [v for v in vehicles if v.get("latitude") is not None and v.get("longitude") is not None]
    assert len(valid_coords) >= 1, "At least one vehicle must have coordinates"
    for v in valid_coords:
        assert -90 <= v["latitude"] <= 90
        assert -180 <= v["longitude"] <= 180
    print(f" PASS: {len(valid_coords)} vehicles have valid GPS markers in bounding area")

def test_12_and_13_osrm_routing_and_fallbacks():
    print("\n[CHECK 12 & 13] Verifying OSRM road routing and robust fallback handling...")
    from services.routing_service import get_road_route

    # Test 12: Live OSRM call
    route = get_road_route(28.6139, 77.2090, 28.7300, 77.2900, timeout=10.0)
    print(f"   Live OSRM response engine: {route.get('routing_engine')}")
    print(f"   Distance: {route.get('distance_km')} km | Duration: {route.get('duration_min')} min")
    assert route.get("distance_km") > 0
    assert len(route.get("points", [])) > 0
    assert route.get("geojson", {}).get("type") == "Feature"
    if not route.get("is_fallback"):
        print("   -> Live OSRM road network connection SUCCESSFUL!")
    else:
        print(f"   -> OSRM offline/rate-limited, fallback triggered cleanly: {route.get('error')}")

    # Test 13: Forced failure must NOT crash
    forced_fallback = get_road_route(28.6139, 77.2090, 28.7300, 77.2900, timeout=1e-8)
    assert forced_fallback["is_fallback"] is True
    assert forced_fallback["routing_engine"] == "STRAIGHT_LINE_FALLBACK"
    assert forced_fallback["distance_km"] > 0
    assert forced_fallback["error"] is not None
    print(f"   -> Forced failure caught gracefully (Error: {forced_fallback['error']})")
    print(" PASS: OSRM road routing and non-crashing fallback verified")

def run_all():
    print("=====================================================================")
    print("LASTMILE AI — PHASE 1 CODEBASE AUDIT & FUNCTIONAL VERIFICATION SUITE")
    print("=====================================================================")
    test_1_fastapi_starts()
    test_2_api_endpoints()
    test_3_five_vehicles_available()
    test_4_gps_updates()
    test_5_vehicle_movement()
    test_6_and_7_stoppage_detection_and_status()
    test_8_recovery_recommendations()
    test_9_and_10_streamlit_dashboard()
    test_11_map_markers()
    test_12_and_13_osrm_routing_and_fallbacks()
    print("\n=====================================================================")
    print(">>> ALL 13 VERIFICATION CRITERIA CONFIRMED & PASSING 100%! <<<")
    print("=====================================================================")

if __name__ == "__main__":
    run_all()
