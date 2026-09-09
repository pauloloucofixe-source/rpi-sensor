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
DB_PATH = os.getenv("DB_PATH", "/data/mina.db")

MQTT_RECONNECT_DELAY = 5
CONTROLLER_STALE_SECONDS = 15


# ============================================================
# MQTT TOPICS
# ============================================================

TOPIC_LEVEL = "mina/water/level"
TOPIC_LITERS = "mina/water/liters"
TOPIC_PUMP_STATUS = "mina/pump/status"
TOPIC_BOREHOLE_STATUS = "mina/borehole/status"
TOPIC_MODE = "mina/system/mode"
TOPIC_HEALTH = "mina/system/health"
TOPIC_METRICS = "mina/system/metrics"
TOPIC_CONFIG_STATUS = "mina/config/status"
TOPIC_SIMULATION_STATUS = "mina/simulation/status"

TOPIC_PUMP_COMMAND = "mina/pump/command"
TOPIC_BOREHOLE_COMMAND = "mina/borehole/command"
TOPIC_MODE_COMMAND = "mina/system/mode/command"
TOPIC_CONFIG_COMMAND = "mina/config/command"
TOPIC_SIMULATION_COMMAND = "mina/simulation/command"


# ============================================================
# APP
# ============================================================

app = Flask(__name__)


# ============================================================
# STATE
# ============================================================

state = {
    "pct": 0.0,
    "litros": 0.0,
    "estado_furo": "DESLIGADO",
    "estado_rega": "DESLIGADA",
    "modo": "AUTO",
    "health": "UNKNOWN",
    "last_update": "",
    "controller_online": False,
}

metrics = {}

simulation = {
    "enabled": False,
    "paused": False,
    "speed": 1.0,
    "level": None,
    "sensor_error": False,
}

config_status = {
    "success": True,
    "error": None,
}

state_lock = threading.Lock()

mqtt_connected = False
mqtt_connected_lock = threading.Lock()

# Este timestamp representa o último HEALTH recebido.
last_health_update = None


# ============================================================
# CONFIGURATION KEYS
# ============================================================

ALLOWED_CONFIG_KEYS = {
    "fundo",
    "limite",
    "largura",
    "comprimento",
    "horas",
    "scheduled_borehole_max_level",
    "irrigation_start_level",
    "irrigation_stop_level",
    "critical_level",
    "recovery_target",
    "normal_borehole_runtime_minutes",
    "max_borehole_runtime_minutes",
    "max_recovery_minutes",
    "measurement_interval_seconds",
    "mock_initial_level",
    "mock_natural_drain_per_minute",
    "mock_borehole_fill_per_minute",
    "mock_irrigation_drain_per_minute",
    "mock_simulation_speed",
}


# ============================================================
# DATABASE
# ============================================================

def db_connect():
    connection = sqlite3.connect(
        DB_PATH,
        timeout=10,
    )

    connection.row_factory = sqlite3.Row

    connection.execute(
        "PRAGMA busy_timeout=10000"
    )

    # O web container tem /data montado como read-only.
    #
    # O controller é o único processo responsável por
    # escrever na SQLite.
    #
    # O web nunca deve alterar journal_mode.
    connection.execute(
        "PRAGMA query_only=ON"
    )

    return connection


# ============================================================
# DATE
# ============================================================

def format_event_time(
    value,
    output_format="%Y-%m-%d %H:%M:%S",
):
    if value is None:
        return ""

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(
            value
        ).strftime(output_format)

    if isinstance(value, str):
        value = value.strip()

        try:
            return datetime.fromtimestamp(
                float(value)
            ).strftime(output_format)
        except ValueError:
            pass

        try:
            parsed = datetime.fromisoformat(value)

            return parsed.strftime(
                output_format
            )

        except ValueError:
            return value

    return str(value)


# ============================================================
# HISTORY
# ============================================================

def get_history():
    connection = None

    try:
        connection = db_connect()

        rows = connection.execute(
            """
            SELECT
                timestamp,
                level,
                liters
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
            "History error: %s",
            exc,
        )

        return []

    finally:
        if connection is not None:
            connection.close()


# ============================================================
# EVENTS
# ============================================================

def get_events(limit=100):
    connection = None

    try:
        limit = max(
            1,
            min(int(limit), 500),
        )

        connection = db_connect()

        rows = connection.execute(
            """
            SELECT
                timestamp,
                event_type,
                message
            FROM events
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        return [
            {
                "ts": row["timestamp"],
                "hora": format_event_time(
                    row["timestamp"]
                ),
                "tipo": row["event_type"],
                "msg": row["message"],
            }
            for row in rows
        ]

    except (
        sqlite3.Error,
        ValueError,
        TypeError,
    ) as exc:

        logger.exception(
            "Events error: %s",
            exc,
        )

        return []

    finally:
        if connection is not None:
            connection.close()


# ============================================================
# ALERTS
# ============================================================

ALERT_TYPES = (
    "ALERT",
    "SAFETY",
    "SENSOR_ERROR",
    "RECOVERY",
)


def get_alerts(limit=50):
    connection = None

    try:
        limit = max(
            1,
            min(int(limit), 200),
        )

        placeholders = ",".join(
            "?" for _ in ALERT_TYPES
        )

        connection = db_connect()

        rows = connection.execute(
            f"""
            SELECT
                timestamp,
                type,
                message
            FROM events
            WHERE type IN ({placeholders})
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (*ALERT_TYPES, limit),
        ).fetchall()

        return [
            {
                "ts": row["timestamp"],
                "hora": format_event_time(
                    row["timestamp"]
                ),
                "tipo": row["type"],
                "msg": row["message"],
            }
            for row in rows
        ]

    except (
        sqlite3.Error,
        ValueError,
        TypeError,
    ) as exc:

        logger.exception(
            "Alerts error: %s",
            exc,
        )

        return []

    finally:
        if connection is not None:
            connection.close()


# ============================================================
# CONFIG - READ ONLY
# ============================================================

def get_config():
    connection = None

    try:
        connection = db_connect()

        rows = connection.execute(
            """
            SELECT
                key,
                value
            FROM config
            ORDER BY key
            """
        ).fetchall()

        result = {}

        for row in rows:
            try:
                result[row["key"]] = json.loads(
                    row["value"]
                )

            except (
                TypeError,
                json.JSONDecodeError,
            ):
                logger.warning(
                    "Invalid config key: %s",
                    row["key"],
                )

        return result

    except sqlite3.Error as exc:
        logger.exception(
            "Config read error: %s",
            exc,
        )

        return {}

    finally:
        if connection is not None:
            connection.close()


# ============================================================
# CONFIG VALIDATION
# ============================================================

def validate_config_payload(data):
    if not isinstance(data, dict):
        raise ValueError(
            "Configuration must be a JSON object."
        )

    if not data:
        raise ValueError(
            "Configuration cannot be empty."
        )

    unknown_keys = (
        set(data)
        - ALLOWED_CONFIG_KEYS
    )

    if unknown_keys:
        raise ValueError(
            "Unknown configuration keys: "
            + ", ".join(
                sorted(unknown_keys)
            )
        )

    if "horas" in data:
        hours = data["horas"]

        if not isinstance(hours, list):
            raise ValueError(
                "horas must be a list."
            )

        if not all(
            isinstance(hour, int)
            and 0 <= hour <= 23
            for hour in hours
        ):
            raise ValueError(
                "horas must contain values "
                "between 0 and 23."
            )

    level_keys = {
        "scheduled_borehole_max_level",
        "irrigation_start_level",
        "irrigation_stop_level",
        "critical_level",
        "recovery_target",
    }

    for key in level_keys:
        if key not in data:
            continue

        value = float(data[key])

        if not 0 <= value <= 100:
            raise ValueError(
                f"{key} must be between 0 and 100."
            )

    dimension_keys = {
        "fundo",
        "limite",
        "largura",
        "comprimento",
    }

    for key in dimension_keys:
        if key not in data:
            continue

        value = float(data[key])

        if value <= 0:
            raise ValueError(
                f"{key} must be positive."
            )

    runtime_keys = {
        "normal_borehole_runtime_minutes",
        "max_borehole_runtime_minutes",
        "max_recovery_minutes",
        "measurement_interval_seconds",
    }

    for key in runtime_keys:
        if key not in data:
            continue

        value = float(data[key])

        if value <= 0:
            raise ValueError(
                f"{key} must be positive."
            )

    mock_keys = {
        "mock_initial_level",
        "mock_natural_drain_per_minute",
        "mock_borehole_fill_per_minute",
        "mock_irrigation_drain_per_minute",
        "mock_simulation_speed",
    }

    for key in mock_keys:
        if key not in data:
            continue

        value = float(data[key])

        if value < 0:
            raise ValueError(
                f"{key} must be >= 0."
            )

    return True


# ============================================================
# MQTT CONNECTION STATE
# ============================================================

def is_mqtt_connected():
    with mqtt_connected_lock:
        return mqtt_connected


def set_mqtt_connected(value):
    global mqtt_connected

    with mqtt_connected_lock:
        mqtt_connected = value


# ============================================================
# CONTROLLER ONLINE
# ============================================================

def is_controller_online():
    if not is_mqtt_connected():
        return False

    with state_lock:
        health = state["health"]
        update = last_health_update

    if update is None:
        return False

    if health != "ONLINE":
        return False

    return (
        time.time() - update
        <= CONTROLLER_STALE_SECONDS
    )


# ============================================================
# MQTT PUBLISH
# ============================================================

def mqtt_publish(
    topic,
    value,
    retain=False,
):
    if not is_mqtt_connected():
        logger.warning(
            "MQTT disconnected: %s",
            topic,
        )

        return False

    try:
        if isinstance(value, (dict, list)):
            payload = json.dumps(
                value,
                separators=(",", ":"),
            )
        else:
            payload = str(value)

        result = mqtt_client.publish(
            topic,
            payload,
            qos=1,
            retain=retain,
        )

        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.error(
                "MQTT publish failed: %s -> %s",
                topic,
                result.rc,
            )

            return False

        logger.info(
            "MQTT command: %s -> %s",
            topic,
            payload,
        )

        return True

    except Exception as exc:
        logger.exception(
            "MQTT publish error: %s",
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
                "MQTT subscription failed: %s",
                result,
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
    global last_health_update

    topic = message.topic

    try:
        value = message.payload.decode(
            "utf-8"
        ).strip()

    except UnicodeDecodeError:
        logger.warning(
            "Invalid UTF-8 MQTT payload: %s",
            topic,
        )

        return

    # --------------------------------------------------------
    # Parse JSON whenever the topic is expected to contain it.
    # --------------------------------------------------------

    parsed = value

    if topic in {
        TOPIC_LEVEL,
        TOPIC_LITERS,
        TOPIC_PUMP_STATUS,
        TOPIC_BOREHOLE_STATUS,
        TOPIC_MODE,
        TOPIC_HEALTH,
        TOPIC_METRICS,
        TOPIC_CONFIG_STATUS,
        TOPIC_SIMULATION_STATUS,
    }:
        try:
            parsed = json.loads(value)

        except json.JSONDecodeError:
            parsed = value

    try:
        with state_lock:

            # ------------------------------------------------
            # WATER LEVEL
            # ------------------------------------------------

            if topic == TOPIC_LEVEL:

                if isinstance(parsed, dict):
                    level = parsed.get(
                        "level"
                    )
                else:
                    level = parsed

                state["pct"] = float(level)

            # ------------------------------------------------
            # WATER LITERS
            # ------------------------------------------------

            elif topic == TOPIC_LITERS:

                if isinstance(parsed, dict):
                    liters = parsed.get(
                        "liters"
                    )
                else:
                    liters = parsed

                state["litros"] = float(liters)

            # ------------------------------------------------
            # PUMP
            # ------------------------------------------------

            elif topic == TOPIC_PUMP_STATUS:

                if isinstance(parsed, dict):
                    active = bool(
                        parsed.get(
                            "active",
                            False,
                        )
                    )

                    state["estado_rega"] = (
                        "LIGADA"
                        if active
                        else "DESLIGADA"
                    )

                else:
                    state["estado_rega"] = str(
                        parsed
                    )

            # ------------------------------------------------
            # BOREHOLE
            # ------------------------------------------------

            elif topic == TOPIC_BOREHOLE_STATUS:

                if isinstance(parsed, dict):
                    active = bool(
                        parsed.get(
                            "active",
                            False,
                        )
                    )

                    state["estado_furo"] = (
                        "LIGADO"
                        if active
                        else "DESLIGADO"
                    )

                else:
                    state["estado_furo"] = str(
                        parsed
                    )

            # ------------------------------------------------
            # SYSTEM MODE
            # ------------------------------------------------

            elif topic == TOPIC_MODE:

                if isinstance(parsed, dict):
                    state["modo"] = str(
                        parsed.get(
                            "mode",
                            "AUTO",
                        )
                    )

                else:
                    state["modo"] = str(
                        parsed
                    )

            # ------------------------------------------------
            # HEALTH
            # ------------------------------------------------

            elif topic == TOPIC_HEALTH:

                if isinstance(parsed, dict):
                    health = parsed.get(
                        "status",
                        "UNKNOWN",
                    )
                else:
                    health = parsed

                state["health"] = str(
                    health
                ).upper()

                # Only HEALTH updates the
                # controller heartbeat.
                last_health_update = (
                    time.time()
                )

            # ------------------------------------------------
            # METRICS
            # ------------------------------------------------

            elif topic == TOPIC_METRICS:

                if not isinstance(
                    parsed,
                    dict,
                ):
                    raise ValueError(
                        "Metrics payload must be an object"
                    )

                metrics.clear()
                metrics.update(parsed)

            # ------------------------------------------------
            # CONFIG STATUS
            # ------------------------------------------------

            elif topic == TOPIC_CONFIG_STATUS:

                if not isinstance(
                    parsed,
                    dict,
                ):
                    raise ValueError(
                        "Config status payload must be an object"
                    )

                config_status.clear()
                config_status.update(parsed)

            # ------------------------------------------------
            # SIMULATION
            # ------------------------------------------------

            elif topic == TOPIC_SIMULATION_STATUS:

                if not isinstance(
                    parsed,
                    dict,
                ):
                    raise ValueError(
                        "Simulation status payload must be an object"
                    )

                simulation.clear()
                simulation.update(parsed)

            else:
                return

    except (
        ValueError,
        TypeError,
        KeyError,
    ) as exc:

        logger.warning(
            "Invalid MQTT value on %s: %s",
            topic,
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


# ============================================================
# MQTT LOOP
# ============================================================

def mqtt_loop():
    while True:
        try:
            logger.info(
                "Connecting to MQTT %s:%s",
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


# ============================================================
# HEALTH
# ============================================================

@app.route("/api/health")
def api_health():
    controller_online = (
        is_controller_online()
    )

    return jsonify(
        {
            "status": "OK",
            "mqtt_connected":
                is_mqtt_connected(),
            "controller_online":
                controller_online,
        }
    )


# ============================================================
# STATUS
# ============================================================

@app.route("/api/status")
def api_status():
    with state_lock:
        result = dict(state)

        result["simulation"] = dict(
            simulation
        )

        health_update = (
            last_health_update
        )

    controller_online = (
        is_controller_online()
    )

    if health_update is not None:
        seconds_since_update = max(
            0,
            time.time() - health_update,
        )

        result["last_update"] = (
            datetime.fromtimestamp(
                health_update
            ).strftime(
                "%H:%M:%S"
            )
        )

        result["last_update_epoch"] = (
            health_update
        )

        result["seconds_since_update"] = round(
            seconds_since_update,
            1,
        )

    result["mqtt_connected"] = (
        is_mqtt_connected()
    )

    result["controller_online"] = (
        controller_online
    )

    return jsonify(result)


# ============================================================
# METRICS
# ============================================================

@app.route("/api/metrics")
def api_metrics():
    with state_lock:
        result = dict(metrics)

    return jsonify(result)


# ============================================================
# HISTORY
# ============================================================

@app.route("/api/historico")
def api_historico():
    return jsonify(
        get_history()
    )


# ============================================================
# EVENTS
# ============================================================

@app.route("/api/eventos")
def api_eventos():
    limit = request.args.get(
        "limit",
        100,
    )

    return jsonify(
        get_events(limit)
    )


# ============================================================
# ALERTS
# ============================================================

@app.route("/api/alerts")
def api_alerts():
    limit = request.args.get(
        "limit",
        50,
    )

    return jsonify(
        get_alerts(limit)
    )


# ============================================================
# CONFIG - GET
# ============================================================

@app.route("/api/config")
def api_config():
    return jsonify(
        get_config()
    )


# ============================================================
# CONFIG - UPDATE
# ============================================================

@app.route(
    "/api/config",
    methods=["POST"],
)
def api_config_update():
    data = request.get_json(
        silent=True
    )

    if (
        not isinstance(data, dict)
        or not data
    ):
        return jsonify(
            {
                "success": False,
                "error": "Invalid JSON",
            }
        ), 400

    try:
        validate_config_payload(
            data
        )

    except (
        ValueError,
        TypeError,
    ) as exc:

        return jsonify(
            {
                "success": False,
                "error": str(exc),
            }
        ), 400

    if not mqtt_publish(
        TOPIC_CONFIG_COMMAND,
        data,
    ):
        return jsonify(
            {
                "success": False,
                "error":
                    "Controller unavailable",
            }
        ), 503

    return jsonify(
        {
            "success": True,
            "pending": True,
            "message":
                "Configuration sent to controller.",
        }
    ), 202


# ============================================================
# CONFIG - RESET
# ============================================================

@app.route(
    "/api/config/reset",
    methods=["POST"],
)
def api_config_reset():
    payload = {
        "action": "RESET",
    }

    if not mqtt_publish(
        TOPIC_CONFIG_COMMAND,
        payload,
    ):
        return jsonify(
            {
                "success": False,
                "error":
                    "Controller unavailable",
            }
        ), 503

    return jsonify(
        {
            "success": True,
            "pending": True,
        }
    ), 202


# ============================================================
# PUMP COMMAND
# ============================================================

@app.route(
    "/api/comando_bomba",
    methods=["POST"],
)
def comando_bomba():
    data = (
        request.get_json(
            silent=True
        )
        or {}
    )

    action = (
        str(
            data.get(
                "acao",
                "auto",
            )
        )
        .lower()
        .strip()
    )

    commands = {
        "ligar": "ON",
        "parar": "OFF",
        "auto": "AUTO",
    }

    command = commands.get(
        action
    )

    if command is None:
        return jsonify(
            {
                "success": False,
                "error":
                    "Invalid command",
            }
        ), 400

    if not mqtt_publish(
        TOPIC_PUMP_COMMAND,
        command,
    ):
        return jsonify(
            {
                "success": False,
                "error":
                    "Controller unavailable",
            }
        ), 503

    return jsonify(
        {
            "success": True,
            "command": command,
        }
    )


# ============================================================
# BOREHOLE COMMAND
# ============================================================

@app.route(
    "/api/comando_furo",
    methods=["POST"],
)
def comando_furo():
    data = (
        request.get_json(
            silent=True
        )
        or {}
    )

    action = (
        str(
            data.get(
                "acao",
                "parar",
            )
        )
        .lower()
        .strip()
    )

    commands = {
        "ligar": "ON",
        "parar": "OFF",
    }

    command = commands.get(
        action
    )

    if command is None:
        return jsonify(
            {
                "success": False,
                "error":
                    "Invalid command",
            }
        ), 400

    if not mqtt_publish(
        TOPIC_BOREHOLE_COMMAND,
        command,
    ):
        return jsonify(
            {
                "success": False,
                "error":
                    "Controller unavailable",
            }
        ), 503

    return jsonify(
        {
            "success": True,
            "command": command,
        }
    )


# ============================================================
# SYSTEM MODE COMMAND
# ============================================================

@app.route(
    "/api/comando_modo",
    methods=["POST"],
)
def comando_modo():
    data = (
        request.get_json(
            silent=True
        )
        or {}
    )

    mode = (
        str(
            data.get(
                "modo",
                "",
            )
        )
        .upper()
        .strip()
    )

    allowed_modes = {
        "AUTO",
        "MANUAL_ON",
        "MANUAL_OFF",
    }

    if mode not in allowed_modes:
        return jsonify(
            {
                "success": False,
                "error":
                    "Invalid mode",
            }
        ), 400

    if not mqtt_publish(
        TOPIC_MODE_COMMAND,
        mode,
    ):
        return jsonify(
            {
                "success": False,
                "error":
                    "Controller unavailable",
            }
        ), 503

    return jsonify(
        {
            "success": True,
            "mode": mode,
        }
    )


# ============================================================
# SIMULATION
# ============================================================

ALLOWED_SIMULATION_ACTIONS = {
    "SET_LEVEL",
    "RESET",
    "PAUSE",
    "RESUME",
    "SPEED",
    "SCENARIO",
    "SENSOR_ERROR",
}


@app.route(
    "/api/simulation",
    methods=["POST"],
)
def api_simulation():
    data = request.get_json(
        silent=True
    )

    if not isinstance(data, dict):
        return jsonify(
            {
                "success": False,
                "error": "Invalid JSON",
            }
        ), 400

    action = (
        str(
            data.get(
                "action",
                "",
            )
        )
        .upper()
        .strip()
    )

    if action not in ALLOWED_SIMULATION_ACTIONS:
        return jsonify(
            {
                "success": False,
                "error":
                    "Invalid simulation action",
            }
        ), 400

    # --------------------------------------------------------
    # SET LEVEL
    # --------------------------------------------------------

    if action == "SET_LEVEL":

        if "level" not in data:
            return jsonify(
                {
                    "success": False,
                    "error":
                        "level is required",
                }
            ), 400

        try:
            level = float(
                data["level"]
            )

            if not 0 <= level <= 100:
                raise ValueError

        except (
            ValueError,
            TypeError,
        ):
            return jsonify(
                {
                    "success": False,
                    "error":
                        "level must be between 0 and 100",
                }
            ), 400

        data["level"] = level

    # --------------------------------------------------------
    # SPEED
    # --------------------------------------------------------

    elif action == "SPEED":

        if "value" not in data:
            return jsonify(
                {
                    "success": False,
                    "error":
                        "value is required",
                }
            ), 400

        try:
            speed = float(
                data["value"]
            )

            if speed <= 0:
                raise ValueError

        except (
            ValueError,
            TypeError,
        ):
            return jsonify(
                {
                    "success": False,
                    "error":
                        "speed must be greater than 0",
                }
            ), 400

        data["value"] = speed

    # --------------------------------------------------------
    # SCENARIO
    # --------------------------------------------------------

    elif action == "SCENARIO":

        allowed_scenarios = {
            "CRITICAL",
            "EMPTY",
            "FULL",
            "NORMAL",
        }

        scenario = (
            str(
                data.get(
                    "name",
                    "",
                )
            )
            .upper()
            .strip()
        )

        if scenario not in allowed_scenarios:
            return jsonify(
                {
                    "success": False,
                    "error":
                        "Invalid scenario",
                }
            ), 400

        data["name"] = scenario

    # --------------------------------------------------------
    # SENSOR ERROR
    # --------------------------------------------------------

    elif action == "SENSOR_ERROR":

        if "enabled" in data:
            if not isinstance(
                data["enabled"],
                bool,
            ):
                return jsonify(
                    {
                        "success": False,
                        "error":
                            "enabled must be boolean",
                    }
                ), 400

    # --------------------------------------------------------
    # SEND COMMAND
    # --------------------------------------------------------

    if not mqtt_publish(
        TOPIC_SIMULATION_COMMAND,
        data,
    ):
        return jsonify(
            {
                "success": False,
                "error":
                    "Controller unavailable",
            }
        ), 503

    return jsonify(
        {
            "success": True,
            "pending": True,
            "command": data,
        }
    ), 202


# ============================================================
# SIMULATION STATE
# ============================================================

@app.route(
    "/api/simulation",
    methods=["GET"],
)
def api_simulation_status():
    with state_lock:
        result = dict(simulation)

    return jsonify(result)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
        threaded=True,
    )