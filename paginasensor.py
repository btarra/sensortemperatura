from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict

import altair as alt
import pandas as pd
import paho.mqtt.client as mqtt
import streamlit as st

# ----------------------------- Configuración -----------------------------
BROKER_HOST = os.getenv("MQTT_HOST", "broker.hivemq.com")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_TLS = os.getenv("MQTT_TLS", "false").lower() in ("1", "true", "yes")
# Usa un tópico propio en producción. Por defecto se conserva el del proyecto.
TOPIC_FILTER = os.getenv("MQTT_TOPIC_FILTER", "esp32/temperatura/tarjeta1")
LEGACY_TOPIC = "esp32/temperatura/tarjeta1"
CLIENT_ID = os.getenv("MQTT_CLIENT_ID", f"dashboard-{uuid.uuid4().hex[:10]}")
DB_PATH = os.getenv("MQTT_DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "historial_mqtt.db"))
TZ = ZoneInfo("America/Bogota")
REFRESH = "2s"
RAM_LIMIT = 5000
LIVE_POINTS = 600
PAGE_SIZE = 100
TIMEOUT_SECONDS = 8

st.set_page_config(page_title="Monitoreo IoT", page_icon="🌡️", layout="wide")
st.title("🌡️ Dashboard de monitoreo IoT")
st.caption("ESP32 · DS18B20 · MQTT · SQLite · Streamlit")


def now_local() -> datetime:
    return datetime.now(TZ)


def parse_topic(topic: str) -> tuple[str, str]:
    parts = topic.split("/")
    if len(parts) >= 3:
        variable, device = parts[-2], parts[-1]
    else:
        variable, device = "temperatura", "dispositivo_desconocido"
    return variable.strip() or "variable", device.strip() or "dispositivo"


def normalize_message(topic: str, payload: bytes) -> tuple[str, str, float, str, datetime]:
    variable, device = parse_topic(topic)
    raw = payload.decode("utf-8").strip()
    unit = "°C" if "temp" in variable.lower() else ""
    timestamp = now_local()
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        obj = None
    if isinstance(obj, dict):
        value = obj.get("value", obj.get("valor", obj.get("temperature", obj.get("temperatura"))))
        device = str(obj.get("device_id", obj.get("dispositivo", device)))
        variable = str(obj.get("variable", obj.get("sensor", variable)))
        unit = str(obj.get("unit", obj.get("unidad", unit)))
        if obj.get("timestamp"):
            parsed = pd.Timestamp(obj["timestamp"])
            timestamp = parsed.to_pydatetime()
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=TZ)
            else:
                timestamp = timestamp.astimezone(TZ)
    else:
        value = raw
    value = float(value)
    if not math.isfinite(value):
        raise ValueError("Valor no finito")
    # Rango físico de referencia DS18B20; variables no térmicas no heredan este límite.
    if "temp" in variable.lower() and not -55 <= value <= 125:
        raise ValueError("Temperatura fuera del rango del DS18B20 (-55 a 125 °C)")
    if len(device) > 80 or len(variable) > 80:
        raise ValueError("Identificador demasiado largo")
    return device, variable, value, unit, timestamp
