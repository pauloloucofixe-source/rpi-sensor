import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime

from flask import Flask, jsonify, render_template, request
import paho.mqtt.client as mqtt


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger("mina-web")


# ============================================================
# CONFIG
# ============================================================

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

DB_PATH = os.getenv(
    "DB_PATH",
    "/data/mina.db",
)

MQTT_RECONNECT_DELAY = 5


# ============================================================
# MQTT TOPICS
# ============================================================

TOPIC_LEVEL = "mina/water/level"
TOPIC_LITERS = "mina/water/liters"

TOPIC_PUMP_STATUS = "mina/pump/status"
TOPIC_BOREHOLE_STATUS = "mina/borehole/status"

TOPIC_MODE = "mina/system/mode"
TOPIC_HEALTH = "mina/system/health"

TOPIC_PUMP_COMMAND = "mina/pump/command"
TOPIC_BOREHOLE_COMMAND = "mina/borehole/command"
TOPIC_MODE_COMMAND = "mina/system/mode/command"


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# CURRENT MQTT STATE
# ============================================================

state = {
    "pct": 0.0,
    "litros": 0,
    "estado_furo": "DESLIGADO",
    "estado_rega": "DESLIGADA",
    "modo": "AUTO",
    "health": "UNKNOWN",
    "last_update": "",
}

state_lock = threading.Lock()

mqtt_connected = False
mqtt_connected_lock = threading.Lock()

last_mqtt_update = None


# ============================================================
# DATABASE
# ============================================================

def db_connect():
    connection = sqlite3.connect(
        DB_PATH,
        timeout=10,
    )

    connection.row_factory = sqlite3.Row

    return connection


def format_event_time(value, output_format="%Y-%m-%d %H:%M:%S"):
    if value is None:
        return ""

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value).strftime(output_format)

    if isinstance(value, str):
        value = value.strip()

        # Try Unix timestamp stored as a string
        try:
            return datetime.fromtimestamp(
                float(value)
            ).strftime(output_format)
        except ValueError:
            pass

        # Try ISO datetime
        try:
            parsed = datetime.fromisoformat(value)
            return parsed.strftime(output_format)
        except ValueError:
            return value

    return str(value)


def get_history():
    connection = None

    try:
        connection = db_connect()

        rows = connection.execute(
            """
            SELECT timestamp, level, liters
            FROM measurements
            ORDER BY timestamp ASC
            """
        ).fetchall()

        return [
            {
                "ts": row["timestamp"],
                "hora": format_event_time(
                    row["timestamp"],
                    "%H:%M",
                ),
                "val": row["level"],
                "litros": row["liters"],
            }
            for row in rows
        ]

    except sqlite3.Error as exc:
        logger.exception(
            "Error reading measurement history: %s",
            exc,
        )
        return []

    finally:
        if connection is not None:
            connection.close()


def get_events():
    connection = None

    try:
        connection = db_connect()

        rows = connection.execute(
            """
            SELECT timestamp, type, message
            FROM events
            ORDER BY timestamp DESC
            LIMIT 100
            """
        ).fetchall()

        return [
            {
                "ts": row["timestamp"],
                "hora": format_event_time(
                    row["timestamp"],
                ),
                "tipo": row["type"],
                "msg": row["message"],
            }
            for row in rows
        ]

    except sqlite3.Error as exc:
        logger.exception(
            "Error reading events: %s",
            exc,
        )
        return []

    finally:
        if connection is not None:
            connection.close()


def get_config():
    connection = None

    try:
        connection = db_connect()

        rows = connection.execute(
            """
            SELECT key, value
            FROM config
            """
        ).fetchall()

        result = {}

        for row in rows:
            try:
                result[row["key"]] = json.loads(
                    row["value"]
                )
            except (TypeError, json.JSONDecodeError):
                logger.warning(
                    "Invalid JSON in config key '%s'",
                    row["key"],
                )

        return result

    except sqlite3.Error as exc:
        logger.exception(
            "Error reading configuration: %s",
            exc,
        )
        return {}

    finally:
        if connection is not None:
            connection.close()


def update_config(data):
    connection = None

    try:
        connection = db_connect()

        for key, value in data.items():
            connection.execute(
                """
                INSERT INTO config(key, value)
                VALUES (?, ?)
                ON CONFLICT(key)
                DO UPDATE SET value=excluded.value
                """,
                (
                    key,
                    json.dumps(value),
                ),
            )

        connection.commit()

        return True

    except sqlite3.Error as exc:
        logger.exception(
            "Error updating configuration: %s",
            exc,
        )

        if connection is not None:
            connection.rollback()

        return False

    finally:
        if connection is not None:
            connection.close()


# ============================================================
# MQTT STATE HELPERS
# ============================================================

def is_mqtt_connected():
    with mqtt_connected_lock:
        return mqtt_connected


def set_mqtt_connected(value):
    global mqtt_connected

    with mqtt_connected_lock:
        mqtt_connected = value


def mqtt_publish(topic, value):
    if not is_mqtt_connected():
        logger.warning(
            "Cannot publish MQTT command because MQTT is disconnected: %s",
            topic,
        )
        return False

    try:
        result = mqtt_client.publish(
            topic,
            str(value),
            qos=1,
            retain=False,
        )

        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.error(
                "MQTT publish failed: topic=%s rc=%s",
                topic,
                result.rc,
            )
            return False

        logger.info(
            "MQTT command published: %s -> %s",
            topic,
            value,
        )

        return True

    except Exception as exc:
        logger.exception(
            "MQTT publish exception: %s",
            exc,
        )
        return False


# ============================================================
# MQTT CALLBACKS
# ============================================================

def on_connect(
    client,
    userdata,
    flags,
    reason_code,
    properties,
):
    logger.info(
        "MQTT connected: %s",
        reason_code,
    )

    set_mqtt_connected(True)

    try:
        result, _ = client.subscribe(
            "mina/#",
            qos=1,
        )

        if result != mqtt.MQTT_ERR_SUCCESS:
            logger.error(
                "Failed to subscribe to mina/#: %s",
                result,
            )
        else:
            logger.info(
                "Subscribed to mina/#",
            )

    except Exception as exc:
        logger.exception(
            "MQTT subscription error: %s",
            exc,
        )


def on_disconnect(
    client,
    userdata,
    disconnect_flags,
    reason_code,
    properties,
):
    set_mqtt_connected(False)

    logger.warning(
        "MQTT disconnected: %s",
        reason_code,
    )


def on_message(
    client,
    userdata,
    message,
):
    global last_mqtt_update

    topic = message.topic

    try:
        value = message.payload.decode("utf-8").strip()
    except UnicodeDecodeError:
        logger.warning(
            "Invalid MQTT payload on topic %s",
            topic,
        )
        return

    try:
        with state_lock:

            if topic == TOPIC_LEVEL:
                state["pct"] = float(value)

            elif topic == TOPIC_LITERS:
                state["litros"] = int(float(value))

            elif topic == TOPIC_PUMP_STATUS:
                state["estado_rega"] = value

            elif topic == TOPIC_BOREHOLE_STATUS:
                state["estado_furo"] = value

            elif topic == TOPIC_MODE:
                state["modo"] = value

            elif topic == TOPIC_HEALTH:
                state["health"] = value

            else:
                return

            last_mqtt_update = time.time()

    except (ValueError, TypeError) as exc:
        logger.warning(
            "Invalid MQTT value: topic=%s value=%r error=%s",
            topic,
            value,
            exc,
        )


# ============================================================
# MQTT CLIENT
# ============================================================

mqtt_client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="mina-web",
)

mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_message = on_message


def mqtt_loop():
    while True:

        try:
            logger.info(
                "Connecting to MQTT broker %s:%s",
                MQTT_HOST,
                MQTT_PORT,
            )

            mqtt_client.connect(
                MQTT_HOST,
                MQTT_PORT,
                keepalive=60,
            )

            mqtt_client.loop_forever()

        except Exception as exc:
            set_mqtt_connected(False)

            logger.warning(
                "MQTT connection error: %s",
                exc,
            )

            time.sleep(
                MQTT_RECONNECT_DELAY
            )


mqtt_thread = threading.Thread(
    target=mqtt_loop,
    name="mqtt-loop",
    daemon=True,
)

mqtt_thread.start()


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():
    return render_template(
        "index.html"
    )


@app.route("/api/health")
def api_health():
    return jsonify(
        {
            "status": "OK",
            "mqtt_connected": is_mqtt_connected(),
        }
    )


@app.route("/api/status")
def api_status():
    with state_lock:
        result = dict(state)
        mqtt_update = last_mqtt_update

    if mqtt_update is not None:
        result["last_update"] = datetime.fromtimestamp(
            mqtt_update
        ).strftime("%H:%M:%S")

    result["mqtt_connected"] = is_mqtt_connected()

    return jsonify(result)


@app.route("/api/historico")
def api_historico():
    return jsonify(
        get_history()
    )


@app.route("/api/eventos")
def api_eventos():
    return jsonify(
        get_events()
    )


@app.route("/api/config")
def api_config():
    return jsonify(
        get_config()
    )


@app.route(
    "/api/config",
    methods=["POST"],
)
def api_config_update():
    data = request.get_json(
        silent=True
    )

    if not isinstance(data, dict) or not data:
        return jsonify(
            {
                "success": False,
                "error": "Invalid JSON",
            }
        ), 400

    if not update_config(data):
        return jsonify(
            {
                "success": False,
                "error": "Could not update configuration",
            }
        ), 500

    return jsonify(
        {
            "success": True,
        }
    )


# ============================================================
# PUMP COMMANDS
# ============================================================

@app.route(
    "/api/comando_bomba",
    methods=["POST"],
)
def comando_bomba():
    data = request.get_json(
        silent=True
    ) or {}

    action = str(
        data.get(
            "acao",
            "auto",
        )
    ).lower().strip()

    commands = {
        "ligar": "ON",
        "parar": "OFF",
        "auto": "AUTO",
    }

    command = commands.get(action)

    if command is None:
        return jsonify(
            {
                "success": False,
                "error": "Invalid command",
            }
        ), 400

    if not mqtt_publish(
        TOPIC_PUMP_COMMAND,
        command,
    ):
        return jsonify(
            {
                "success": False,
                "error": "Controller unavailable",
            }
        ), 503

    return jsonify(
        {
            "success": True,
            "command": command,
        }
    )


# ============================================================
# BOREHOLE COMMANDS
# ============================================================

@app.route(
    "/api/comando_furo",
    methods=["POST"],
)
def comando_furo():
    data = request.get_json(
        silent=True
    ) or {}

    action = str(
        data.get(
            "acao",
            "parar",
        )
    ).lower().strip()

    commands = {
        "ligar": "ON",
        "parar": "OFF",
    }

    command = commands.get(action)

    if command is None:
        return jsonify(
            {
                "success": False,
                "error": "Invalid command",
            }
        ), 400

    if not mqtt_publish(
        TOPIC_BOREHOLE_COMMAND,
        command,
    ):
        return jsonify(
            {
                "success": False,
                "error": "Controller unavailable",
            }
        ), 503

    return jsonify(
        {
            "success": True,
            "command": command,
        }
    )


# ============================================================
# SYSTEM MODE
# ============================================================

@app.route(
    "/api/comando_modo",
    methods=["POST"],
)
def comando_modo():
    data = request.get_json(
        silent=True
    ) or {}

    mode = str(
        data.get(
            "modo",
            "",
        )
    ).upper().strip()

    allowed_modes = {
        "AUTO",
        "MANUAL_ON",
        "MANUAL_OFF",
    }

    if mode not in allowed_modes:
        return jsonify(
            {
                "success": False,
                "error": "Invalid mode",
            }
        ), 400

    if not mqtt_publish(
        TOPIC_MODE_COMMAND,
        mode,
    ):
        return jsonify(
            {
                "success": False,
                "error": "Controller unavailable",
            }
        ), 503

    return jsonify(
        {
            "success": True,
            "mode": mode,
        }
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
        threaded=True,
    )