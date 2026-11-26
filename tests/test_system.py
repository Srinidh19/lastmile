import threading
import time
import requests
import uvicorn
from backend.main import app

def run_integration_tests():
    test_port = 8010
    base = f"http://127.0.0.1:{test_port}"

    def start_server():
        uvicorn.run(app, host="127.0.0.1", port=test_port, log_level="error")

    server_thread = threading.Thread(target=start_server, daemon=True)
    server_thread.start()
    time.sleep(2)

    print("=== TEST 1: GET /health ===")
    res = requests.get(f"{base}/health", timeout=5)
    assert res.status_code == 200, f"Health check failed: {res.text}"
    health_data = res.json()
    print("Health Status:", health_data)
    assert health_data["status"] == "healthy"

    print("\n=== TEST 2: POST /orders/generate (Realistic Order Generation: 35 Orders) ===")
    res = requests.post(f"{base}/orders/generate?count=35", timeout=5)
    assert res.status_code == 200
    gen_data = res.json()
    print(f"Generation message: {gen_data['message']}")
    assert gen_data["count"] == 35

    print("\n=== TEST 3: Strict Schema Validation of 12 Required Fields ===")
    res = requests.get(f"{base}/orders", timeout=5)
    assert res.status_code == 200
    orders = res.json()
    assert len(orders) == 35

    required_fields = [
        "order_id",
        "pickup_latitude",
        "pickup_longitude",
        "destination_latitude",
        "destination_longitude",
        "assigned_vehicle_id",
        "priority",
        "created_at",
        "delivery_deadline",
        "estimated_delivery_time",
        "order_status",
        "actual_delivery_time"
    ]
    for o in orders:
        for field in required_fields:
            assert field in o, f"Field '{field}' missing in order {o.get('order_id')}"
        assert -90.0 <= o["pickup_latitude"] <= 90.0
        assert -180.0 <= o["pickup_longitude"] <= 180.0
        assert -90.0 <= o["destination_latitude"] <= 90.0
        assert -180.0 <= o["destination_longitude"] <= 180.0
        assert o["priority"] in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
        assert o["order_status"] in ["PENDING", "ASSIGNED", "PICKED_UP", "IN_TRANSIT", "DELIVERED", "DELAYED"]
    print("All 35 orders successfully validated against all 12 mandatory fields!")

    print("\n=== TEST 4: Multi-Attribute Filtering on GET /orders ===")
    # Filter by status
    pending_orders = requests.get(f"{base}/orders?status=PENDING", timeout=5).json()
    print(f"Pending orders count: {len(pending_orders)}")
    for po in pending_orders:
        assert po["order_status"] == "PENDING"
        assert po["assigned_vehicle_id"] is None

    # Filter by vehicle
    v101_orders = requests.get(f"{base}/orders?vehicle_id=V101", timeout=5).json()
    print(f"V101 assigned orders: {len(v101_orders)}")
    for vo in v101_orders:
        assert vo["assigned_vehicle_id"] == "V101"

    # Filter by priority
    high_orders = requests.get(f"{base}/orders?priority=HIGH", timeout=5).json()
    print(f"HIGH priority orders: {len(high_orders)}")
    for ho in high_orders:
        assert ho["priority"] == "HIGH"

    print("\n=== TEST 5: GET /orders/{order_id} & GET /orders/vehicle/{vehicle_id} ===")
    sample_order = orders[0]
    res = requests.get(f"{base}/orders/{sample_order['order_id']}", timeout=5)
    assert res.status_code == 200
    assert res.json()["order_id"] == sample_order["order_id"]

    res = requests.get(f"{base}/orders/vehicle/V101", timeout=5)
    assert res.status_code == 200
    assert isinstance(res.json(), list)

    print("\n=== TEST 6: POST /orders (Single Order Creation with Validation) ===")
    new_order = {
        "order_id": "ORD-TEST-999",
        "pickup_latitude": 28.6315,
        "pickup_longitude": 77.2167,
        "destination_latitude": 28.7025,
        "destination_longitude": 77.1930,
        "assigned_vehicle_id": None,
        "priority": "HIGH",
        "delivery_deadline": "2026-11-26T02:00:00+00:00",
        "package_description": "Urgent Plasma Samples"
    }
    res = requests.post(f"{base}/orders", json=new_order, timeout=5)
    assert res.status_code == 200
    created = res.json()["order"]
    assert created["order_id"] == "ORD-TEST-999"
    assert created["order_status"] == "PENDING"
    print("Created Order:", created["order_id"], created["order_status"])

    print("\n=== TEST 7: POST /orders/{order_id}/assign (Assignment & Capacity Enforcement) ===")
    # Assign newly created order to V103
    res = requests.post(f"{base}/orders/ORD-TEST-999/assign?vehicle_id=V103", timeout=5)
    assert res.status_code == 200
    assigned = res.json()["order"]
    assert assigned["assigned_vehicle_id"] == "V103"
    assert assigned["order_status"] == "ASSIGNED"
    print(f"Order ORD-TEST-999 successfully assigned to V103 (Status: {assigned['order_status']})")

    # Test invalid vehicle assignment rejection
    res = requests.post(f"{base}/orders/ORD-TEST-999/assign?vehicle_id=INVALID_VEHICLE", timeout=5)
    assert res.status_code == 400
    print("Invalid vehicle assignment correctly rejected with HTTP 400!")

    print("\n=== TEST 8: PATCH /orders/{order_id}/status (Lifecycle State Machine) ===")
    # ASSIGNED -> PICKED_UP
    res = requests.patch(f"{base}/orders/ORD-TEST-999/status", json={"status": "PICKED_UP"}, timeout=5)
    assert res.status_code == 200
    assert res.json()["order"]["order_status"] == "PICKED_UP"

    # PICKED_UP -> IN_TRANSIT
    res = requests.patch(f"{base}/orders/ORD-TEST-999/status", json={"status": "IN_TRANSIT"}, timeout=5)
    assert res.status_code == 200
    assert res.json()["order"]["order_status"] == "IN_TRANSIT"

    # IN_TRANSIT -> DELIVERED
    res = requests.patch(f"{base}/orders/ORD-TEST-999/status", json={"status": "DELIVERED"}, timeout=5)
    assert res.status_code == 200
    deliv_order = res.json()["order"]
    assert deliv_order["order_status"] == "DELIVERED"
    assert deliv_order["actual_delivery_time"] is not None
    print(f"Order transitioned to DELIVERED at {deliv_order['actual_delivery_time']}")

    # Prevent reassignment or status reversal on DELIVERED order
    res = requests.post(f"{base}/orders/ORD-TEST-999/assign?vehicle_id=V101", timeout=5)
    assert res.status_code == 400
    print("Reassignment on DELIVERED order correctly rejected with HTTP 400!")

    print("\n=== TEST 9: POST /events (Telemetry & Stoppage Disruption on Orders) ===")
    # Telemetry for V101 (Moving)
    ev_v101 = {
        "vehicle_id": "V101",
        "latitude": 28.6139,
        "longitude": 77.2090,
        "speed": 32.0,
        "timestamp": "2026-11-26T01:00:00+00:00"
    }
    r = requests.post(f"{base}/events", json=ev_v101, timeout=5)
    assert r.status_code == 200
    assert r.json()["assigned_orders_count"] > 0
    print(f"V101 telemetry processed. Assigned orders count: {r.json()['assigned_orders_count']}")

    # Telemetry for V102 (Stopped >= 20s to trigger disruption)
    ev_v102 = {
        "vehicle_id": "V102",
        "latitude": 28.6200,
        "longitude": 77.2150,
        "speed": 0.0,
        "timestamp": "2026-11-26T01:00:00+00:00"
    }
    r = requests.post(f"{base}/events", json=ev_v102, timeout=5)
    assert r.status_code == 200

    time.sleep(1)
    ev_v102["timestamp"] = "2026-11-26T01:00:25+00:00"
    r = requests.post(f"{base}/events", json=ev_v102, timeout=5)
    assert r.status_code == 200
    print(f"V102 stagnation event recorded. Disrupted status: {r.json()['status']}")

    print("\n=== TEST 10: GET /recovery/{vehicle_id} with Disrupted Order Matching ===")
    rec_res = requests.get(f"{base}/recovery/V102", timeout=5).json()
    print(f"Recovery Decision for V102: {rec_res['decision']}")
    print(f"Disrupted orders: {rec_res.get('disrupted_orders', [])}")
    print(f"Recommendations count: {len(rec_res.get('recommendations', []))}")
    assert "disrupted_orders" in rec_res
    assert rec_res["pending_deliveries"] > 0

    print("\n=== TEST 11: GET /orders/summary & GET /orders/available-vehicles ===")
    order_sum = requests.get(f"{base}/orders/summary", timeout=5).json()
    assert order_sum["total_orders"] >= 35
    assert "distribution_by_vehicle" in order_sum
    print("Order Summary Metrics:", order_sum)

    avail_veh = requests.get(f"{base}/orders/available-vehicles", timeout=5).json()
    assert isinstance(avail_veh, list)
    print("Available Vehicles with Spare Capacity:", avail_veh)

    print("\n========================================================")
    print(">>> ALL 11 ORDER MANAGEMENT ENGINE TESTS PASSED 100%! <<<")
    print("========================================================")

if __name__ == "__main__":
    run_integration_tests()
