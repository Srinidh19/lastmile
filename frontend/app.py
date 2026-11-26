import os
import sys
from pathlib import Path

# Ensure repository root is in sys.path when running as a nested script
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import streamlit as st
import pandas as pd
import requests
import folium
import html
from datetime import datetime, timezone
from typing import Any, Optional, Dict, List

from streamlit_folium import st_folium
from streamlit_autorefresh import st_autorefresh
from services.routing_service import get_road_route as fetch_road_route

# ============================================================
# 1. CONFIGURATION & CLOUD BACKEND RESOLUTION
# ============================================================

# Resolves backend URL from environment, Streamlit secrets, or defaults to live Render backend
API_URL = os.getenv("API_URL") or os.getenv("BACKEND_URL")
if not API_URL:
    try:
        API_URL = st.secrets.get("API_URL", "https://lastmile-backend-58fc.onrender.com")
    except Exception:
        API_URL = "https://lastmile-backend-58fc.onrender.com"
API_URL = str(API_URL).rstrip("/")

st.set_page_config(
    page_title="LastMile AI | Order & Fleet Control Tower",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# 2. PROFESSIONAL DARK THEME & STYLES
# ============================================================

st.markdown("""
<style>

.stApp {
    background-color: #0B1120;
    color: #E5E7EB;
}

[data-testid="stHeader"] {
    background-color: #0B1120;
}

[data-testid="stSidebar"] {
    background-color: #111827;
    border-right: 1px solid #263247;
}

[data-testid="stSidebar"] * {
    color: #E5E7EB;
}

.block-container {
    padding-top: 1.5rem;
    padding-bottom: 2rem;
    max-width: 1550px;
}

h1, h2, h3 {
    color: #F8FAFC !important;
}

p, label {
    color: #CBD5E1;
}

.kpi-card {
    background: linear-gradient(145deg, #172338, #111827);
    border: 1px solid #263750;
    border-radius: 12px;
    padding: 16px;
    min-height: 105px;
}

.kpi-label {
    color: #94A3B8;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.5px;
    text-transform: uppercase;
}

.kpi-value {
    color: #F8FAFC;
    font-size: 26px;
    font-weight: 800;
    margin-top: 6px;
}

.kpi-caption {
    color: #64748B;
    font-size: 11px;
    margin-top: 5px;
}

.section-heading {
    color: #F1F5F9;
    font-size: 18px;
    font-weight: 700;
    margin-top: 14px;
    margin-bottom: 12px;
}

.alert-card {
    background: #172338;
    border: 1px solid #334155;
    border-left: 4px solid #F59E0B;
    border-radius: 9px;
    padding: 14px;
    margin-bottom: 10px;
}

.alert-critical {
    border-left-color: #EF4444;
}

.alert-medium {
    border-left-color: #F59E0B;
}

.vehicle-load-card {
    background: #131E31;
    border: 1px solid #24354D;
    border-radius: 10px;
    padding: 12px 14px;
    text-align: center;
}

.vehicle-load-title {
    font-size: 13px;
    font-weight: 700;
    color: #E2E8F0;
}

.vehicle-load-stat {
    font-size: 18px;
    font-weight: 800;
    color: #38BDF8;
    margin: 4px 0;
}

.vehicle-load-sub {
    font-size: 10px;
    color: #94A3B8;
}

.footer {
    color: #64748B;
    font-size: 11px;
    text-align: center;
    padding: 20px;
}

hr {
    border-color: #263247;
}

.stButton > button {
    background-color: #2563EB;
    color: white;
    border: none;
    border-radius: 8px;
    font-weight: 600;
}

.stButton > button:hover {
    background-color: #1D4ED8;
    color: white;
}

</style>
""", unsafe_allow_html=True)


# ============================================================
# 3. API CLIENT FUNCTIONS
# ============================================================

def fetch_data(endpoint: str):
    """Fetch JSON data from backend with error handling."""
    try:
        response = requests.get(f"{API_URL}{endpoint}", timeout=4)
        response.raise_for_status()
        return response.json()
    except requests.RequestException:
        return None


def post_generate_orders(count: int = 35):
    """Trigger realistic order generation on backend."""
    try:
        url = f"{API_URL}/orders/generate"
        resp = requests.post(url, params={"count": count}, timeout=5)
        return resp.json() if resp.status_code == 200 else None
    except requests.RequestException:
        return None


def post_assign_order(order_id: str, vehicle_id: str):
    """Assign or reassign an order to a vehicle."""
    try:
        url = f"{API_URL}/orders/{order_id}/assign"
        resp = requests.post(url, params={"vehicle_id": vehicle_id}, timeout=4)
        return resp.json() if resp.status_code == 200 else None
    except requests.RequestException:
        return None


def patch_order_status(order_id: str, new_status: str):
    """Update order lifecycle status."""
    try:
        url = f"{API_URL}/orders/{order_id}/status"
        resp = requests.patch(url, json={"status": new_status}, timeout=4)
        return resp.json() if resp.status_code == 200 else None
    except requests.RequestException:
        return None


# ============================================================
# 4. CACHED ROAD ROUTING WRAPPER
# ============================================================

@st.cache_data(ttl=45, show_spinner=False)
def get_cached_route(start_lat: float, start_lon: float, end_lat: float, end_lon: float):
    """
    Cached wrapper around the robust routing service.
    Returns real road-following route or straight-line fallback with metadata.
    """
    return fetch_road_route(
        start_lat=start_lat,
        start_lon=start_lon,
        end_lat=end_lat,
        end_lon=end_lon,
        timeout=8.0
    )


# ============================================================
# 5. UI COMPONENTS
# ============================================================

def display_kpi(label: str, value: Any, caption: str):
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-caption">{caption}</div>
        </div>
        """,
        unsafe_allow_html=True
    )


# ============================================================
# 6. SIDEBAR SETTINGS
# ============================================================

st.sidebar.markdown(
    """
    <div style="padding:10px 0 15px 0;">
        <div style="font-size:24px;font-weight:800;color:#F8FAFC;">
            🚚 LASTMILE AI
        </div>
        <div style="font-size:11px;color:#94A3B8;letter-spacing:0.5px;">
            ORDER DISPATCH & MOBILITY CONTROL TOWER
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

st.sidebar.divider()
st.sidebar.subheader("Dashboard Refresh")

refresh_rate = st.sidebar.slider(
    "Refresh interval (seconds)",
    min_value=5,
    max_value=30,
    value=8
)

st.sidebar.caption("Backend: FastAPI v1.2.0")
st.sidebar.caption("Backbone: Condense + Kafka Streaming")
st.sidebar.caption("Order Engine: Dynamic 35+ Order Queue")
st.sidebar.caption("Routing: OSRM Road Driving + Fallback")
st.sidebar.caption("Map Engine: Folium / OpenStreetMap")
st.sidebar.divider()
st.sidebar.caption("LastMile AI Platform · Delhi NCR Fleet Operations")

# Automatic refresh trigger
st_autorefresh(
    interval=refresh_rate * 1000,
    key="dashboard_refresh"
)


# ============================================================
# 7. HEADER & STREAMING BACKBONE STATUS
# ============================================================

streaming_data = fetch_data("/streaming/status") or {}
is_kafka = streaming_data.get("kafka_connected", False)
stream_mode = streaming_data.get("streaming_mode", "ACTIVE")
total_events = (
    streaming_data.get("producer_stats", {}).get("total_published", 0) +
    streaming_data.get("consumer_stats", {}).get("total_consumed", 0)
)

header_col1, header_col2 = st.columns([3, 1])

with header_col1:
    st.markdown(
        """
        <div style="font-size:26px;font-weight:800;color:#F8FAFC;">
            Mobility & Order Intelligence Control Tower
        </div>
        <div style="font-size:13px;color:#94A3B8;margin-top:4px;">
            Real-time fleet tracking, 25-50 delivery order lifecycle management, road-following OSRM navigation, and dynamic vehicle recovery.
        </div>
        """,
        unsafe_allow_html=True
    )

with header_col2:
    badge_bg = "#123326" if is_kafka else "#1E293B"
    badge_color = "#4ADE80" if is_kafka else "#38BDF8"
    badge_title = "CONDENSE + KAFKA STREAMING" if is_kafka else "REAL-TIME STREAMING ACTIVE"
    st.markdown(
        f"""
        <div style="
            background:{badge_bg};
            color:{badge_color};
            padding:10px;
            border-radius:8px;
            text-align:center;
            font-size:12px;
            font-weight:700;
            border:1px solid #334155;">
            ● {badge_title}
            <div style="font-size:10px;color:#94A3B8;margin-top:2px;">
                Mode: {stream_mode} | Events: {total_events}
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

st.write("")


# ============================================================
# 8. FETCH BACKEND DATA
# ============================================================

summary = fetch_data("/summary")
vehicles = fetch_data("/vehicles")
alerts = fetch_data("/alerts")
orders = fetch_data("/orders")
order_summary = fetch_data("/orders/summary")

if summary is None or vehicles is None or alerts is None or orders is None:
    st.error(
        "⚠️ Backend connection failed. Please ensure the FastAPI backend is running on port 8000."
    )
    st.code("uvicorn backend.main:app --reload --port 8000")
    st.stop()

if not isinstance(vehicles, list):
    vehicles = []
if not isinstance(alerts, list):
    alerts = []
if not isinstance(orders, list):
    orders = []
if not isinstance(order_summary, dict):
    order_summary = {}

# Index active orders by assigned vehicle
vehicle_to_orders = {}
for o in orders:
    vid = o.get("assigned_vehicle_id")
    if vid:
        if vid not in vehicle_to_orders:
            vehicle_to_orders[vid] = []
        vehicle_to_orders[vid].append(o)


# ============================================================
# 9. ORDER & FLEET INTELLIGENCE METRICS
# ============================================================

st.markdown('<div class="section-heading">📦 Order Intelligence Overview</div>', unsafe_allow_html=True)

m1, m2, m3, m4, m5, m6 = st.columns(6)

with m1:
    display_kpi("TOTAL ORDERS", order_summary.get("total_orders", len(orders)), "Orders in ecosystem")

with m2:
    display_kpi("PENDING", order_summary.get("pending", 0), "Unassigned in queue")

with m3:
    display_kpi("ASSIGNED", order_summary.get("assigned", 0), "Allocated to fleet")

with m4:
    display_kpi("IN TRANSIT", order_summary.get("in_transit", 0), "Active on road")

with m5:
    display_kpi("DELIVERED", order_summary.get("delivered", 0), "Successfully dropped off")

with m6:
    display_kpi("DELAYED (SLA RISK)", order_summary.get("delayed", 0), "Disrupted / at risk")

st.write("")


# ============================================================
# 10. ORDER DISTRIBUTION BY VEHICLE & CAPACITY ALLOCATION
# ============================================================

st.markdown('<div class="section-heading">🚛 Fleet Order Distribution & Capacity Allocation</div>', unsafe_allow_html=True)

fleet_counts = order_summary.get("distribution_by_vehicle", {})
dist_cols = st.columns(6)

vehicle_ids = ["V101", "V102", "V103", "V104", "V105"]

for idx, vid in enumerate(vehicle_ids):
    with dist_cols[idx]:
        count = fleet_counts.get(vid, 0)
        spare = max(0, 10 - count)
        st.markdown(
            f"""
            <div class="vehicle-load-card">
                <div class="vehicle-load-title">Vehicle {vid}</div>
                <div class="vehicle-load-stat">{count} / 10</div>
                <div class="vehicle-load-sub">{spare} spare slots available</div>
            </div>
            """,
            unsafe_allow_html=True
        )

with dist_cols[5]:
    unassigned_count = order_summary.get("unassigned_orders_count", 0)
    st.markdown(
        f"""
        <div class="vehicle-load-card" style="border-color:#F59E0B;">
            <div class="vehicle-load-title" style="color:#F59E0B;">Unassigned Queue</div>
            <div class="vehicle-load-stat" style="color:#F59E0B;">{unassigned_count}</div>
            <div class="vehicle-load-sub">Awaiting fleet dispatch</div>
        </div>
        """,
        unsafe_allow_html=True
    )

st.write("")
st.divider()


# ============================================================
# 11. ORDER MANAGEMENT TABLE & MULTI-ATTRIBUTE FILTERS
# ============================================================

st.markdown('<div class="section-heading">📋 Order Management & Dispatch Registry</div>', unsafe_allow_html=True)

# Filter Controls & Actions Header
action_c1, action_c2 = st.columns([3, 1])

with action_c1:
    f_col1, f_col2, f_col3 = st.columns(3)
    with f_col1:
        status_filter = st.selectbox(
            "Filter by Order Status",
            ["ALL", "PENDING", "ASSIGNED", "PICKED_UP", "IN_TRANSIT", "DELIVERED", "DELAYED"],
            index=0
        )
    with f_col2:
        priority_filter = st.selectbox(
            "Filter by Priority",
            ["ALL", "CRITICAL", "HIGH", "MEDIUM", "LOW"],
            index=0
        )
    with f_col3:
        vehicle_filter = st.selectbox(
            "Filter by Vehicle Assignment",
            ["ALL", "V101", "V102", "V103", "V104", "V105", "UNASSIGNED"],
            index=0
        )

with action_c2:
    st.write("")
    if st.button("🎲 Generate 35 Realistic Orders", use_container_width=True):
        gen_res = post_generate_orders(count=35)
        if gen_res:
            st.success("Generated 35 fresh realistic orders!")
            st.rerun()
        else:
            st.error("Failed to generate orders.")

# Filter dataset
filtered_orders = orders[:]
if status_filter != "ALL":
    filtered_orders = [o for o in filtered_orders if o.get("order_status", "").upper() == status_filter]
if priority_filter != "ALL":
    filtered_orders = [o for o in filtered_orders if o.get("priority", "").upper() == priority_filter]
if vehicle_filter != "ALL":
    if vehicle_filter == "UNASSIGNED":
        filtered_orders = [o for o in filtered_orders if not o.get("assigned_vehicle_id")]
    else:
        filtered_orders = [o for o in filtered_orders if (o.get("assigned_vehicle_id") or "").upper() == vehicle_filter]

# Render Dataframe
if filtered_orders:
    order_rows = []
    for o in filtered_orders:
        pri = o.get("priority", "MEDIUM")
        pri_tag = "🔴 CRITICAL" if pri == "CRITICAL" else "🟠 HIGH" if pri == "HIGH" else "🟡 MEDIUM" if pri == "MEDIUM" else "🟢 LOW"

        stat = o.get("order_status", "PENDING")
        stat_tag = (
            "🚚 In Transit" if stat == "IN_TRANSIT"
            else "📦 Picked Up" if stat == "PICKED_UP"
            else "📋 Assigned" if stat == "ASSIGNED"
            else "✅ Delivered" if stat == "DELIVERED"
            else "⚠️ Delayed" if stat == "DELAYED"
            else "⏳ Pending"
        )

        pickup_addr = o.get("pickup_address") or (o.get("pickup_location", {}).get("address") if isinstance(o.get("pickup_location"), dict) else f"({o.get('pickup_latitude', 0):.4f}, {o.get('pickup_longitude', 0):.4f})")
        dest_addr = o.get("destination_address") or (o.get("destination", {}).get("address") if isinstance(o.get("destination"), dict) else f"({o.get('destination_latitude', 0):.4f}, {o.get('destination_longitude', 0):.4f})")

        deadline_str = (o.get("delivery_deadline") or "")[:19].replace("T", " ")
        eta_str = (o.get("estimated_delivery_time") or "")[:19].replace("T", " ")
        actual_str = (o.get("actual_delivery_time") or "-")
        if actual_str != "-":
            actual_str = actual_str[:19].replace("T", " ")

        order_rows.append({
            "Order ID": o.get("order_id"),
            "Priority": pri_tag,
            "Status": stat_tag,
            "Vehicle": o.get("assigned_vehicle_id") or "Unassigned",
            "Cargo Description": o.get("package_description", "Standard Cargo"),
            "Pickup Hub": pickup_addr,
            "Delivery Destination": dest_addr,
            "SLA Deadline": deadline_str,
            "Estimated ETA": eta_str,
            "Actual Delivery": actual_str
        })

    st.dataframe(pd.DataFrame(order_rows), use_container_width=True, hide_index=True, height=260)
    st.caption(f"Showing {len(filtered_orders)} of {len(orders)} total orders in registry.")
else:
    st.info("No orders matched the selected filter criteria.")

# Quick Order Assignment Expander
with st.expander("⚡ Dispatch & Assign Pending Orders to Available Vehicles", expanded=False):
    unassigned_orders = [o for o in orders if not o.get("assigned_vehicle_id") and o.get("order_status") != "DELIVERED"]
    if unassigned_orders:
        assign_col1, assign_col2, assign_col3 = st.columns([2, 1.5, 1])
        with assign_col1:
            selected_order_id = st.selectbox(
                "Select Pending Order",
                [f"{o['order_id']} | {o.get('priority')} | {o.get('package_description')} -> {o.get('destination_address')}" for o in unassigned_orders]
            )
            raw_order_id = selected_order_id.split(" | ")[0]
        with assign_col2:
            target_veh = st.selectbox(
                "Select Vehicle with Spare Capacity",
                [f"{v} ({10 - fleet_counts.get(v, 0)} slots left)" for v in vehicle_ids if 10 - fleet_counts.get(v, 0) > 0]
            )
            raw_target_veh = target_veh.split(" ")[0]
        with assign_col3:
            st.write("")
            st.write("")
            if st.button("Confirm Assignment", use_container_width=True):
                assign_res = post_assign_order(raw_order_id, raw_target_veh)
                if assign_res:
                    st.success(f"Order {raw_order_id} assigned to {raw_target_veh}!")
                    st.rerun()
                else:
                    st.error("Assignment failed.")
    else:
        st.info("All pending orders are currently assigned.")

st.divider()


# ============================================================
# 12. LIVE MAP WITH ROAD ROUTES & ORDER DESTINATIONS
# ============================================================

st.markdown('<div class="section-heading">📍 Live Vehicle Telemetry & Road Routing (OSRM)</div>', unsafe_allow_html=True)

valid_vehicles = [
    v for v in vehicles
    if v.get("latitude") is not None and v.get("longitude") is not None
]

map_col, info_col = st.columns([2.5, 1])
route_results = {}

with map_col:
    if valid_vehicles:
        all_map_points = []
        for v in valid_vehicles:
            all_map_points.append([float(v["latitude"]), float(v["longitude"])])

        # Initialize Folium Map centered on fleet centroid
        center_lat = sum(p[0] for p in all_map_points) / len(all_map_points)
        center_lon = sum(p[1] for p in all_map_points) / len(all_map_points)

        fleet_map = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=12,
            control_scale=True,
            tiles="OpenStreetMap"
        )

        routing_warnings = []

        # Iterate over vehicles and draw routes to their primary active order destination
        for vehicle in valid_vehicles:
            vehicle_id = vehicle.get("vehicle_id")
            status = vehicle.get("status", "UNKNOWN")
            speed = vehicle.get("speed", 0)
            current_lat = float(vehicle["latitude"])
            current_lon = float(vehicle["longitude"])

            # Resolve active order for this vehicle
            assigned_list = vehicle_to_orders.get(vehicle_id, [])
            active_order = next((o for o in assigned_list if o.get("order_status") != "DELIVERED"), None)

            if active_order:
                dest_lat = float(active_order.get("destination_latitude") or active_order.get("destination", {}).get("lat", 28.7000))
                dest_lon = float(active_order.get("destination_longitude") or active_order.get("destination", {}).get("lon", 77.3000))
                dest_name = active_order.get("destination_address") or active_order.get("destination", {}).get("address", f"Delivery for {active_order.get('order_id')}")
                order_id = active_order.get("order_id")
                order_priority = active_order.get("priority", "MEDIUM")
                order_status = active_order.get("order_status", "IN_TRANSIT")
            else:
                dest_lat = 28.7200
                dest_lon = 77.3100
                dest_name = f"Default Holding Hub for {vehicle_id}"
                order_id = "NO_ACTIVE_ORDER"
                order_priority = "LOW"
                order_status = "IDLE"

            all_map_points.append([dest_lat, dest_lon])

            # Obtain road-following route from OSRM (or fallback)
            route = get_cached_route(
                round(current_lat, 4),
                round(current_lon, 4),
                round(dest_lat, 4),
                round(dest_lon, 4)
            )

            if route:
                route_results[vehicle_id] = {
                    **route,
                    "order_id": order_id,
                    "destination_name": dest_name,
                    "order_count": len(assigned_list)
                }

                if route.get("is_fallback"):
                    routing_warnings.append(
                        f"⚠️ {vehicle_id}: Road routing fallback active ({route.get('error')})"
                    )
                    # Render straight-line fallback with dashed amber styling
                    folium.PolyLine(
                        locations=route["points"],
                        color="#F59E0B",
                        weight=4,
                        opacity=0.9,
                        dash_array="6, 8",
                        tooltip=f"{vehicle_id} (Fallback Route) | {route['distance_km']} km | ~{route['duration_min']} min"
                    ).add_to(fleet_map)
                else:
                    # Render true OSRM road route with solid blue styling
                    folium.PolyLine(
                        locations=route["points"],
                        color="#2563EB",
                        weight=5,
                        opacity=0.85,
                        tooltip=f"{vehicle_id} (OSRM Road) | {route['distance_km']} km | {route['duration_min']} min"
                    ).add_to(fleet_map)

            # Vehicle Marker
            marker_color = "green" if status == "MOVING" else "red" if status == "STOPPED" else "blue"
            folium.Marker(
                location=[current_lat, current_lon],
                tooltip=f"Vehicle {vehicle_id} ({status}) | Orders: {len(assigned_list)}",
                popup=folium.Popup(
                    f"""
                    <div style="font-family:sans-serif;font-size:12px;">
                        <b>Vehicle:</b> {html.escape(str(vehicle_id))}<br>
                        <b>Status:</b> {html.escape(str(status))}<br>
                        <b>Speed:</b> {speed} km/h<br>
                        <b>Active Order:</b> {html.escape(str(order_id))}<br>
                        <b>Total Assigned Orders:</b> {len(assigned_list)}<br>
                        <b>Order Status:</b> {html.escape(str(order_status))}
                    </div>
                    """,
                    max_width=250
                ),
                icon=folium.Icon(color=marker_color, icon="truck", prefix="fa")
            ).add_to(fleet_map)

            # Destination Marker
            folium.Marker(
                location=[dest_lat, dest_lon],
                tooltip=f"Destination: {order_id}",
                popup=folium.Popup(
                    f"""
                    <div style="font-family:sans-serif;font-size:12px;">
                        <b>Order ID:</b> {html.escape(str(order_id))}<br>
                        <b>Destination:</b> {html.escape(str(dest_name))}<br>
                        <b>Assigned Vehicle:</b> {html.escape(str(vehicle_id))}<br>
                        <b>Priority:</b> {html.escape(str(order_priority))}<br>
                        <b>Status:</b> {html.escape(str(order_status))}
                    </div>
                    """,
                    max_width=260
                ),
                icon=folium.Icon(color="red", icon="flag", prefix="fa")
            ).add_to(fleet_map)

        # Also place subtle markers for unassigned order destinations
        unassigned_orders_sample = [o for o in orders if not o.get("assigned_vehicle_id")][:5]
        for u_ord in unassigned_orders_sample:
            u_lat = float(u_ord.get("destination_latitude") or u_ord.get("destination", {}).get("lat", 28.7000))
            u_lon = float(u_ord.get("destination_longitude") or u_ord.get("destination", {}).get("lon", 77.3000))
            all_map_points.append([u_lat, u_lon])
            folium.Marker(
                location=[u_lat, u_lon],
                tooltip=f"Pending Queue: {u_ord.get('order_id')}",
                popup=folium.Popup(f"<b>Unassigned Order:</b> {u_ord.get('order_id')}<br>Priority: {u_ord.get('priority')}", max_width=200),
                icon=folium.Icon(color="orange", icon="clock-o", prefix="fa")
            ).add_to(fleet_map)

        # Fit map bounds to encompass all vehicles and destinations
        if len(all_map_points) > 1:
            fleet_map.fit_bounds(all_map_points, padding=(30, 30))

        st_folium(fleet_map, width=None, height=540, key="fleet_osrm_map")

        if routing_warnings:
            for w in routing_warnings:
                st.caption(w)

    else:
        st.info("No active vehicle telemetry available. Please ensure simulator is running.")


# ============================================================
# 13. MAP LEGEND & ROUTE STATS
# ============================================================

with info_col:
    st.markdown("### MAP LEGEND")
    st.markdown("🟢 **Moving Vehicle** — Speed > 2 km/h")
    st.markdown("🔴 **Stopped Vehicle** — Stagnation alert active")
    st.markdown("🚩 **Delivery Destination** — Active Order Point")
    st.markdown("🕒 **Orange Clock** — Unassigned Pending Dropoff")

    st.markdown(
        """
        <div style="display:flex;align-items:center;margin-top:10px;">
            <div style="width:25px;height:4px;background:#2563EB;border-radius:2px;margin-right:8px;"></div>
            <span style="font-size:12px;color:#E5E7EB;"><b>OSRM Road Route</b> (Live)</span>
        </div>
        <div style="display:flex;align-items:center;margin-top:6px;margin-bottom:12px;">
            <div style="width:25px;height:3px;border-top:2px dashed #F59E0B;margin-right:8px;"></div>
            <span style="font-size:12px;color:#E5E7EB;"><b>Fallback Direct Route</b></span>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.divider()
    st.metric("Fleet Vehicles Online", len(valid_vehicles))
    st.metric("Active Navigation Routes", len(route_results))

    if route_results:
        st.markdown("### ROUTE DISPATCH STATS")
        for vid, rdata in route_results.items():
            engine_badge = "🛣️ OSRM" if not rdata.get("is_fallback") else "⚠️ Fallback"
            st.markdown(f"**{vid}** · Order `{rdata.get('order_id')}` ({rdata.get('order_count')} on board)")
            st.caption(f"{engine_badge} · Distance: {rdata.get('distance_km')} km · ETA: {rdata.get('duration_min')} min")
            if rdata.get("is_fallback"):
                st.caption(f"<span style='color:#F59E0B;'>Reason: {html.escape(str(rdata.get('error')))}</span>", unsafe_allow_html=True)
            st.write("")

st.divider()


# ============================================================
# 14. INCIDENT & ALERT CENTRE
# ============================================================

st.markdown('<div class="section-heading">🚨 Disruption & Alert Centre</div>', unsafe_allow_html=True)

if alerts:
    for alert in alerts:
        severity = str(alert.get("severity", "MEDIUM")).upper()
        vehicle_id = html.escape(str(alert.get("vehicle_id", "Unknown")))
        alert_type = html.escape(str(alert.get("alert_type", "Unknown Alert")))
        message = html.escape(str(alert.get("message", "No description")))
        risk_score = html.escape(str(alert.get("risk_score", "N/A")))
        stopped_seconds = html.escape(str(alert.get("stopped_seconds", "N/A")))

        alert_class = "alert-critical" if severity == "CRITICAL" else "alert-medium"
        icon = "🔴" if severity == "CRITICAL" else "🟠"

        st.markdown(
            f"""
            <div class="alert-card {alert_class}">
                <b>{icon} {severity} DISRUPTION | Vehicle {vehicle_id}</b>
                <br><br>
                <b>Type:</b> {alert_type}<br>
                <b>Description:</b> {message}<br>
                <b>Risk Score:</b> {risk_score} / 100 &nbsp; | &nbsp; <b>Stopped Duration:</b> {stopped_seconds}s
            </div>
            """,
            unsafe_allow_html=True
        )
else:
    st.success("✅ No active incidents. Fleet operating normally.")

st.divider()


# ============================================================
# 15. DYNAMIC RECOVERY CENTRE
# ============================================================

st.markdown('<div class="section-heading">🔄 Dynamic Vehicle & Order Recovery</div>', unsafe_allow_html=True)

rec_col1, rec_col2 = st.columns([1, 2])
vehicle_ids_online = [v.get("vehicle_id") for v in vehicles if v.get("vehicle_id")]

with rec_col1:
    if vehicle_ids_online:
        selected_vehicle = st.selectbox("Select Target Vehicle for Recovery Analysis", vehicle_ids_online)
        find_recovery = st.button("Calculate Recovery Options", use_container_width=True)
    else:
        selected_vehicle = None
        find_recovery = False
        st.info("No vehicles online.")

with rec_col2:
    if find_recovery and selected_vehicle:
        recovery = fetch_data(f"/recovery/{selected_vehicle}")
        if recovery is None:
            st.error("Unable to contact recovery engine.")
        elif recovery.get("decision") == "NO_RECOVERY_REQUIRED":
            st.info(f"ℹ️ Vehicle {selected_vehicle}: {recovery.get('message', 'No recovery required.')}")
        elif recovery.get("recommendations"):
            st.success(f"Recovery Plan Ready for {selected_vehicle}")
            st.markdown(f"**Disrupted Orders:** `{', '.join(recovery.get('disrupted_orders', []))}`")
            st.markdown(f"**Recoverable Orders:** {recovery.get('orders_recoverable')} / {recovery.get('pending_deliveries')}")

            for plan in recovery.get("recommendations", []):
                cand_id = plan["vehicle_id"]
                cand_dist = plan["distance_km"]
                reassign_orders = plan.get("order_ids", [])

                with st.expander(f"Candidate Vehicle: {cand_id} ({cand_dist} km away)", expanded=True):
                    st.write(f"Spare Capacity: {plan['available_capacity']} slots")
                    st.write(f"Orders to Reassign: {', '.join(reassign_orders)}")

                    # Execution button for recovery
                    if st.button(f"⚡ Execute Reassignment to {cand_id}", key=f"reassign_{cand_id}"):
                        success_count = 0
                        for oid in reassign_orders:
                            res = post_assign_order(oid, cand_id)
                            if res:
                                success_count += 1
                        if success_count > 0:
                            st.success(f"Successfully reassigned {success_count} order(s) to {cand_id}!")
                            st.rerun()
                        else:
                            st.error("Failed to execute reassignment.")
        else:
            st.warning("No eligible candidate vehicles with spare capacity found within recovery radius.")
    else:
        st.info("Select a stationary vehicle to compute proximity-based load shedding and recovery options.")

st.divider()


# ============================================================
# 16. FOOTER
# ============================================================

st.markdown(
    """
    <div class="footer">
        LASTMILE AI · REAL-TIME MOBILITY INTELLIGENCE & ORDER DISPATCH PLATFORM
        <br>
        Road routing powered by Open Source Routing Machine (OSRM) and OpenStreetMap.
        Telemetry simulator active with dynamic order synchronization.
    </div>
    """,
    unsafe_allow_html=True
)
