#!/usr/bin/env python3
"""
QuantAQ hourly publisher.

Polls the QuantAQ Cloud API for the most recent reading from each configured
device, normalizes the payload to match the project's shared reading schema,
and publishes each reading to `v1/quantaq/reading` on the MQTT broker.

Designed to run as a short-lived process triggered by a systemd timer (or
equivalent scheduler) once per hour -- it does not stay running between polls.
"""

import json
import logging
import os
import sys
import requests
import argparse
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("quantaq_publisher")

QUANTAQ_API_BASE = os.environ.get("QUANTAQ_API_BASE", "https://api.quant-aq.com/v1")
QUANTAQ_API_KEY = os.environ["QUANTAQ_API_KEY"]                          # required
QUANTAQ_NETWORK_ID = os.environ.get("QUANTAQ_NETWORK_ID")                # required
QUANTAQ_ORG_ID = os.environ.get("QUANTAQ_ORG_ID")                        # required

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_TOPIC = os.environ.get("QUANTAQ_PUB_TOPIC", "v1/quantaq/reading")  # required

REQUEST_TIMEOUT_S = 15

def fetch_device_serialnumbers() -> list[str]:
    # Return a list of serial numbers from the collection
    return list(map(
        lambda d: d["sn"], 
        _fetch_devices(org_id=QUANTAQ_ORG_ID, network_id=QUANTAQ_NETWORK_ID)
    ))

def _fetch_devices(**params) -> list[dict]:
    resp = requests.get(
        f"{QUANTAQ_API_BASE}/devices/",
        params=params,
        auth=(QUANTAQ_API_KEY, ""),  # API key as HTTP Basic username, blank password
        timeout=REQUEST_TIMEOUT_S,
    )
    resp.raise_for_status()
    return resp.json()["data"]

def fetch_all_readings(serial_numbers) -> list[dict]:
    readings = []
    for sn in serial_numbers:
        r = _fetch_latest(sn, org_id=QUANTAQ_ORG_ID, network_id=QUANTAQ_NETWORK_ID, limit=1, sort="timestamp,desc")
        readings.append(r)
    return readings

def _fetch_latest(sn, **params) -> list[dict]:
    resp = requests.get(
        f"{QUANTAQ_API_BASE}/devices/{sn}/data/",
        params=params,
        auth=(QUANTAQ_API_KEY, ""),  # API key as HTTP Basic username, blank password
        timeout=REQUEST_TIMEOUT_S,
    )
    resp.raise_for_status()
    return resp.json()["data"][0]

def normalize(record: dict) -> dict:
    """Map a QuantAQ data record onto the project's shared reading schema.

    Field choices worth double-checking against your device model's manual:
    - temperature_c uses temp_manifold (closer to sampled ambient air) rather
      than temp_box (runs warmer from internal electronics).
    """
    geo = record.get("geo") or {}
    met = record.get("met") or {}
    return {
        "source": "quantaq",
        "name": f"QuantAQ SN#{record['sn']}",
        "device_id": record["sn"],
        "timestamp": datetime.now(timezone.utc).isoformat(), 
        "pm1": record.get("pm1"),
        "pm25": record.get("pm25"),
        "pm10": record.get("pm10"),
        "temperature_c": record.get("temp", 0.0),
        "humidity_pct": met.get("rh", 0.0),
        "pressure_pa": 0.0,               # Sensor not supported on MODULAIR/MODULAIR-PM models being used
        "latitude": geo.get("lat"),
        "longitude": geo.get("lon")
    }

def publish(readings: list[dict]) -> None:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="quantaq-publisher")
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_start()

    for reading in readings:
        payload = json.dumps(reading)
        result = client.publish(MQTT_TOPIC, payload, qos=1)
        result.wait_for_publish(timeout=10)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            log.error("Publish failed for %s: rc=%s", reading["device_id"], result.rc)
        else:
            log.info(
                "Published reading for %s at %s", reading["device_id"], reading["timestamp"]
            )

    client.loop_stop()
    client.disconnect()

def main(args) -> int:
    device_serialnumbers = fetch_device_serialnumbers()
    log.info(f"Identified {len(device_serialnumbers)} devices: {device_serialnumbers}")
    while True:
        try:
            raw_records = fetch_all_readings(device_serialnumbers)
            if not raw_records:
                log.warning("No records returned from QuantAQ")

            readings = [normalize(r) for r in raw_records]
            publish(readings)
        except requests.HTTPError as e:
            log.error("QuantAQ API error: %s", e)
            return 1
        except requests.RequestException as e:
            log.error("QuantAQ API request failed: %s", e)
            return 1

        log.info(f"QuantAQ Publisher waiting for {args.timer} seconds")
        time.sleep(args.timer)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t", "--timer", default=3600, 
        help="number of seconds between each query of the QuantAQ API and subsequent publishing cycle (default: 1 hour)"
    )
    args = parser.parse_args()    
    sys.exit(main(args)) # Will only ever exit cleanly with an error status of `1`
