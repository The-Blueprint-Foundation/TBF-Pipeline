import os
from dotenv import load_dotenv
from paho.mqtt import client as mqtt_client
import psycopg2 as db
import json

load_dotenv()

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_TOPIC = os.environ.get("MQTT_TOPICS_CSV", "")
DATABASE_URL = os.environ.get("DATABASE_URL")

def connect_mqtt(host, port: int):
    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            print(f"Connected to MQTT Broker!")
        else:
            print(f"Failed to connect, return code {reason_code}")

    client = mqtt_client.Client(
        client_id="tbf_changeisintheair_subscriber",
        callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2
    )
    
    client.on_connect = on_connect
    client.connect(host, port)
    return client

def process(payload: dict) -> dict:
    # whatever transformation/validation you need
    return payload

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload)
        record = process(data)
        with db.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "CALL air_quality.submit_readings (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        record["device_id"], 
                        record["source"],
                        record["name"],
                        record["longitude"],
                        record["latitude"],
                        record["pm1"],
                        record["pm25"],
                        record["pm10"],
                        record["temperature_c"],
                        record["pressure_pa"],
                        record["humidity_pct"]
                    ),
                )
    except Exception as e:
        print(f"Failed to process message on {msg.topic}: {e}")

def run():    
    client = connect_mqtt(MQTT_HOST, MQTT_PORT)
    client.on_message = on_message
    for topic in MQTT_TOPIC.split(','):
        client.subscribe(topic)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("Exiting...")

if __name__ == '__main__':
    run()