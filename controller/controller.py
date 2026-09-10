import json
import logging
import os
import signal
import sqlite3
import threading
import time
from copy import deepcopy
from datetime import datetime
from statistics import median

import paho.mqtt.client as mqtt


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger("mina-controller")


# ============================================================
# ENVIRONMENT
# ============================================================

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

DB_PATH = os.getenv(
    "DB_PATH",
    "/data/mina.db",
)

MOCK_SENSOR = (
    os.getenv("MOCK_SENSOR", "true").lower()
    == "true"
)

LOOP_INTERVAL_SECONDS = 2

CONFIG_RELOAD_INTERVAL_SECONDS = 5

MQTT_RECONNECT_DELAY_SECONDS = 5


# ============================================================
# GPIO
# ============================================================

SENSOR_ECHO = 24
SENSOR_TRIGGER = 23

VALVE_GPIO = 17
IRRIGATION_PUMP_GPIO = 27


# ============================================================
# MQTT TOPICS
# ============================================================

TOPIC_WATER_LEVEL = "mina/water/level"
TOPIC_WATER_LITERS = "mina/water/liters"

TOPIC_PUMP_STATUS = "mina/pump/status"
TOPIC_BOREHOLE_STATUS = "mina/borehole/status"

TOPIC_SYSTEM_MODE = "mina/system/mode"
TOPIC_SYSTEM_HEALTH = "mina/system/health"
TOPIC_SYSTEM_METRICS = "mina/system/metrics"

TOPIC_PUMP_COMMAND = "mina/pump/command"
TOPIC_BOREHOLE_COMMAND = "mina/borehole/command"
TOPIC_MODE_COMMAND = "mina/system/mode/command"

TOPIC_CONFIG_COMMAND = "mina/config/command"
TOPIC_CONFIG_STATUS = "mina/config/status"

TOPIC_SIMULATION_COMMAND = "mina/simulation/command"
TOPIC_SIMULATION_STATUS = "mina/simulation/status"


# ============================================================
# CONFIGURATION
# ============================================================

DEFAULT_CONFIG = {
    "fundo": 0.35,
    "limite": 0.20,
    "largura": 0.15,
    "comprimento": 0.15,

    "horas": [3, 6, 11, 15, 18, 23],
    "scheduled_borehole_max_level": 80.0,

    "irrigation_start_level": 20.0,
    "irrigation_stop_level": 15.0,

    "critical_level": 10.0,
    "recovery_target": 20.0,

    "normal_borehole_runtime_minutes": 10,
    "max_borehole_runtime_minutes": 30,
    "max_recovery_minutes": 120,

    "measurement_interval_seconds": 60,

    "mock_initial_level": 50.0,
    "mock_natural_drain_per_minute": 0.15,
    "mock_borehole_fill_per_minute": 2.0,
    "mock_irrigation_drain_per_minute": 1.0,
    "mock_simulation_speed": 1.0,
}


ALLOWED_CONFIG_KEYS = set(DEFAULT_CONFIG.keys())


# ------------------------------------------------------------
# Backwards compatibility / migration
# ------------------------------------------------------------

LEGACY_CONFIG_KEYS = {
    "tank_fundo": "fundo",
    "tank_limite": "limite",
    "tank_largura": "largura",
    "tank_comprimento": "comprimento",

    "borehole_schedule": "horas",

    "recovery_target_level": "recovery_target",

    "borehole_runtime_minutes":
        "normal_borehole_runtime_minutes",

    "borehole_max_runtime_minutes":
        "max_borehole_runtime_minutes",

    "borehole_max_recovery_minutes":
        "max_recovery_minutes",
}


# ============================================================
# GLOBAL STATE
# ============================================================

running = True

config = deepcopy(DEFAULT_CONFIG)

config_lock = threading.RLock()
db_lock = threading.RLock()
state_lock = threading.RLock()


current_level = None
current_liters = None

pump_active = False
borehole_active = False

system_mode = "AUTO"

controller_started_at = time.time()

last_loop_time = None
last_measurement_time = 0
last_config_reload = 0

last_scheduled_run = None

borehole_started_at = None
borehole_reason = None

recovery_started_at = None

sensor_failure_active = False

simulation_paused = False
simulation_sensor_error = False

last_simulation_publish = 0
last_mqtt_publish = 0


# ============================================================
# METRICS
# ============================================================

metrics = {
    "loop_count": 0,
    "loop_errors": 0,
    "sensor_errors": 0,
    "control_errors": 0,
    "mqtt_messages": 0,
    "measurements_saved": 0,
    "events_saved": 0,

    "pump_starts": 0,
    "pump_stops": 0,

    "borehole_starts": 0,
    "borehole_stops": 0,

    "last_loop_duration_ms": 0,
    "max_loop_duration_ms": 0,

    "last_error": None,
    "last_error_at": None,
}


# ============================================================
# MOCK OUTPUT DEVICE
# ============================================================

class MockOutputDevice:
    def __init__(self, name):
        self.name = name
        self.value = False

    def on(self):
        self.value = True
        logger.info("%s ON", self.name)

    def off(self):
        self.value = False
        logger.info("%s OFF", self.name)


# ============================================================
# GPIO OUTPUTS
# ============================================================

if MOCK_SENSOR:
    valve = MockOutputDevice("Borehole valve")
    irrigation_pump = MockOutputDevice("Irrigation pump")

else:
    try:
        from gpiozero import OutputDevice

        valve = OutputDevice(
            VALVE_GPIO,
            active_high=True,
            initial_value=False,
        )

        irrigation_pump = OutputDevice(
            IRRIGATION_PUMP_GPIO,
            active_high=True,
            initial_value=False,
        )

    except Exception:
        logger.exception(
            "Failed to initialize GPIO. "
            "Falling back to mock outputs."
        )

        valve = MockOutputDevice("Borehole valve")
        irrigation_pump = MockOutputDevice(
            "Irrigation pump"
        )


# ============================================================
# DATABASE
# ============================================================

def db_connect():
    conn = sqlite3.connect(
        DB_PATH,
        timeout=10,
        check_same_thread=False,
    )

    conn.row_factory = sqlite3.Row

    conn.execute(
        "PRAGMA busy_timeout=10000"
    )

    return conn


def serialize_config_value(value):
    return json.dumps(value)


def deserialize_config_value(value):
    try:
        return json.loads(value)
    except Exception:
        return value


# ============================================================
# DATABASE INITIALIZATION
# ============================================================

def init_db():
    with db_lock:
        conn = db_connect()

        try:
            conn.execute("PRAGMA journal_mode=DELETE")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS measurements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    level REAL NOT NULL,
                    liters REAL NOT NULL
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    message TEXT NOT NULL,
                    details TEXT
                )
            """)

            for key, value in DEFAULT_CONFIG.items():
                conn.execute(
                    """
                    INSERT INTO config(key, value)
                    VALUES (?, ?)
                    ON CONFLICT(key) DO NOTHING
                    """,
                    (
                        key,
                        serialize_config_value(value),
                    ),
                )

            conn.commit()

            logger.info("Database initialized")

        finally:
            conn.close()


# ============================================================
# CONFIGURATION
# ============================================================

def get_config_snapshot():
    with config_lock:
        return deepcopy(config)


def validate_config(candidate):
    if not isinstance(candidate, dict):
        raise ValueError(
            "Configuration must be an object"
        )

    missing = (
        ALLOWED_CONFIG_KEYS
        - set(candidate.keys())
    )

    if missing:
        raise ValueError(
            f"Missing configuration keys: "
            f"{sorted(missing)}"
        )

    unknown = (
        set(candidate.keys())
        - ALLOWED_CONFIG_KEYS
    )

    if unknown:
        raise ValueError(
            f"Unknown configuration keys: "
            f"{sorted(unknown)}"
        )

    # --------------------------------------------------------
    # Dimensions
    # --------------------------------------------------------

    for key in (
        "fundo",
        "limite",
        "largura",
        "comprimento",
    ):
        value = candidate[key]

        if (
            isinstance(value, bool)
            or not isinstance(
                value,
                (int, float),
            )
        ):
            raise ValueError(
                f"{key} must be numeric"
            )

        if value < 0:
            raise ValueError(
                f"{key} cannot be negative"
            )

    if candidate["fundo"] <= candidate["limite"]:
        raise ValueError(
            "fundo must be greater than limite"
        )

    if candidate["largura"] <= 0:
        raise ValueError(
            "largura must be greater than zero"
        )

    if candidate["comprimento"] <= 0:
        raise ValueError(
            "comprimento must be greater than zero"
        )

    # --------------------------------------------------------
    # Percentages
    # --------------------------------------------------------

    percentage_keys = (
        "scheduled_borehole_max_level",
        "irrigation_start_level",
        "irrigation_stop_level",
        "critical_level",
        "recovery_target",
        "mock_initial_level",
    )

    for key in percentage_keys:
        value = candidate[key]

        if (
            isinstance(value, bool)
            or not isinstance(
                value,
                (int, float),
            )
        ):
            raise ValueError(
                f"{key} must be numeric"
            )

        if not 0 <= value <= 100:
            raise ValueError(
                f"{key} must be between 0 and 100"
            )

    # --------------------------------------------------------
    # Threshold relationships
    # --------------------------------------------------------

    if (
        candidate["irrigation_stop_level"]
        >= candidate["irrigation_start_level"]
    ):
        raise ValueError(
            "irrigation_stop_level must be "
            "lower than irrigation_start_level"
        )

    if (
        candidate["critical_level"]
        >= candidate["irrigation_stop_level"]
    ):
        raise ValueError(
            "critical_level must be "
            "lower than irrigation_stop_level"
        )

    if (
        candidate["recovery_target"]
        <= candidate["critical_level"]
    ):
        raise ValueError(
            "recovery_target must be "
            "greater than critical_level"
        )

    if (
        candidate["scheduled_borehole_max_level"]
        <= 0
    ):
        raise ValueError(
            "scheduled_borehole_max_level "
            "must be greater than zero"
        )

    # --------------------------------------------------------
    # Runtime configuration
    # --------------------------------------------------------

    runtime_keys = (
        "normal_borehole_runtime_minutes",
        "max_borehole_runtime_minutes",
        "max_recovery_minutes",
        "measurement_interval_seconds",
    )

    for key in runtime_keys:
        value = candidate[key]

        if (
            isinstance(value, bool)
            or not isinstance(
                value,
                (int, float),
            )
        ):
            raise ValueError(
                f"{key} must be numeric"
            )

        if value <= 0:
            raise ValueError(
                f"{key} must be greater than zero"
            )

    if (
        candidate["max_borehole_runtime_minutes"]
        < candidate[
            "normal_borehole_runtime_minutes"
        ]
    ):
        raise ValueError(
            "max_borehole_runtime_minutes must be "
            "greater than or equal to "
            "normal_borehole_runtime_minutes"
        )

    # --------------------------------------------------------
    # Schedule
    # --------------------------------------------------------

    hours = candidate["horas"]

    if not isinstance(hours, list):
        raise ValueError(
            "horas must be a list"
        )

    for hour in hours:
        if (
            isinstance(hour, bool)
            or not isinstance(hour, int)
        ):
            raise ValueError(
                "horas must contain integers"
            )

        if not 0 <= hour <= 23:
            raise ValueError(
                "horas values must be between 0 and 23"
            )

    # --------------------------------------------------------
    # Mock configuration
    # --------------------------------------------------------

    mock_keys = (
        "mock_natural_drain_per_minute",
        "mock_borehole_fill_per_minute",
        "mock_irrigation_drain_per_minute",
        "mock_simulation_speed",
    )

    for key in mock_keys:
        value = candidate[key]

        if (
            isinstance(value, bool)
            or not isinstance(
                value,
                (int, float),
            )
        ):
            raise ValueError(
                f"{key} must be numeric"
            )

        if value < 0:
            raise ValueError(
                f"{key} cannot be negative"
            )

    if candidate["mock_simulation_speed"] <= 0:
        raise ValueError(
            "mock_simulation_speed must be "
            "greater than zero"
        )

    return True


def load_config():
    global config

    with db_lock:
        conn = db_connect()

        try:
            rows = conn.execute(
                """
                SELECT key, value
                FROM config
                """
            ).fetchall()

            loaded = deepcopy(
                DEFAULT_CONFIG
            )

            migrated_keys = {}

            for row in rows:
                raw_key = row["key"]

                key = LEGACY_CONFIG_KEYS.get(
                    raw_key,
                    raw_key,
                )

                if key not in ALLOWED_CONFIG_KEYS:
                    continue

                value = deserialize_config_value(
                    row["value"]
                )

                loaded[key] = value

                if raw_key != key:
                    migrated_keys[
                        raw_key
                    ] = key

            validate_config(loaded)

            # ------------------------------------------------
            # Migrate old configuration names.
            # ------------------------------------------------

            if migrated_keys:
                logger.warning(
                    "Migrating legacy configuration keys: %s",
                    migrated_keys,
                )

                for old_key, new_key in (
                    migrated_keys.items()
                ):
                    conn.execute(
                        """
                        INSERT INTO config(key, value)
                        VALUES (?, ?)
                        ON CONFLICT(key)
                        DO UPDATE SET
                            value = excluded.value
                        """,
                        (
                            new_key,
                            serialize_config_value(
                                loaded[new_key]
                            ),
                        ),
                    )

                    conn.execute(
                        """
                        DELETE FROM config
                        WHERE key = ?
                        """,
                        (old_key,),
                    )

                conn.commit()

                logger.info(
                    "Legacy configuration migrated"
                )

            with config_lock:
                config = loaded

            logger.info(
                "Configuration loaded successfully"
            )

        finally:
            conn.close()


def save_config_changes(changes):
    global config

    with config_lock:
        candidate = deepcopy(config)
        candidate.update(changes)

    validate_config(candidate)

    with db_lock:
        conn = db_connect()

        try:
            for key, value in changes.items():
                if key not in ALLOWED_CONFIG_KEYS:
                    raise ValueError(
                        f"Unknown configuration key: {key}"
                    )

                conn.execute(
                    """
                    INSERT INTO config(key, value)
                    VALUES (?, ?)
                    ON CONFLICT(key)
                    DO UPDATE SET
                        value = excluded.value
                    """,
                    (
                        key,
                        serialize_config_value(
                            value
                        ),
                    ),
                )

            conn.commit()

        finally:
            conn.close()

    with config_lock:
        config = candidate

    save_event(
        "CONFIG",
        "Configuration updated",
        changes,
    )

    publish_config_status(
        success=True,
        error=None,
    )

    logger.info(
        "Configuration updated: %s",
        changes,
    )


def reset_config():
    global config

    validate_config(
        DEFAULT_CONFIG
    )

    with db_lock:
        conn = db_connect()

        try:
            for key, value in DEFAULT_CONFIG.items():
                conn.execute(
                    """
                    INSERT INTO config(key, value)
                    VALUES (?, ?)
                    ON CONFLICT(key)
                    DO UPDATE SET
                        value = excluded.value
                    """,
                    (
                        key,
                        serialize_config_value(
                            value
                        ),
                    ),
                )

            conn.commit()

        finally:
            conn.close()

    with config_lock:
        config = deepcopy(
            DEFAULT_CONFIG
        )

    save_event(
        "CONFIG",
        "Configuration reset",
        DEFAULT_CONFIG,
    )

    publish_config_status(
        success=True,
        error=None,
    )

    logger.info(
        "Configuration reset to defaults"
    )


# ============================================================
# DATABASE EVENTS
# ============================================================

def save_event(event_type, message, details=None):
    timestamp = datetime.now().isoformat()

    details_json = (
        json.dumps(details, ensure_ascii=False)
        if details is not None
        else None
    )

    with db_lock:
        conn = db_connect()

        try:
            conn.execute(
                """
                INSERT INTO events (
                    timestamp,
                    event_type,
                    message,
                    details
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    timestamp,
                    event_type,
                    message,
                    details_json,
                ),
            )

            conn.commit()

            with state_lock:
                metrics["events_saved"] += 1

        except Exception:
            logger.exception("Failed to save event")

        finally:
            conn.close()


def save_measurement(
    level,
    liters,
):
    try:
        with db_lock:
            conn = db_connect()

            try:
                conn.execute(
                    """
                    INSERT INTO measurements(
                        timestamp,
                        level,
                        liters
                    )
                    VALUES (?, ?, ?)
                    """,
                    (
                        datetime.now().isoformat(),
                        float(level),
                        float(liters),
                    ),
                )

                conn.commit()

                metrics[
                    "measurements_saved"
                ] += 1

            finally:
                conn.close()

    except Exception:
        logger.exception(
            "Failed to save measurement"
        )


# ============================================================
# WATER CALCULATIONS
# ============================================================

def calculate_liters(level_percent):
    cfg = get_config_snapshot()

    level = max(
        0.0,
        min(
            100.0,
            float(level_percent),
        ),
    )

    usable_height = (
        cfg["fundo"]
        - cfg["limite"]
    )

    if usable_height <= 0:
        return 0.0

    water_height = (
        usable_height
        * (level / 100.0)
    )

    volume_m3 = (
        cfg["largura"]
        * cfg["comprimento"]
        * water_height
    )

    return volume_m3 * 1000.0


# ============================================================
# MOCK SENSOR
# ============================================================

class MockSensor:
    """
    Simulated water-level sensor.

    The level changes according to:

    - natural drain
    - borehole filling
    - irrigation draining
    - simulation speed
    """

    def __init__(self):
        self.level = None
        self.last_update = time.monotonic()

        self.paused = False
        self.sensor_error = False

        self.lock = threading.RLock()

        self.reset()

    def reset(self):
        cfg = get_config_snapshot()

        with self.lock:
            self.level = float(
                cfg["mock_initial_level"]
            )

            self.last_update = (
                time.monotonic()
            )

            self.paused = False
            self.sensor_error = False

        logger.info(
            "Mock sensor reset: %.2f%%",
            self.level,
        )

    def set_level(self, level):
        with self.lock:
            self.level = max(
                0.0,
                min(
                    100.0,
                    float(level),
                ),
            )

            self.last_update = (
                time.monotonic()
            )

        logger.info(
            "Mock sensor level set to %.2f%%",
            self.level,
        )

    def get_level(self):
        with self.lock:

            if self.sensor_error:
                raise RuntimeError(
                    "Simulated sensor failure"
                )

            now = time.monotonic()

            elapsed_seconds = (
                now
                - self.last_update
            )

            self.last_update = now

            if self.paused:
                return self.level

            if elapsed_seconds <= 0:
                return self.level

            cfg = get_config_snapshot()

            simulation_speed = max(
                0.0,
                float(
                    cfg.get(
                        "mock_simulation_speed",
                        1.0,
                    )
                ),
            )

            if simulation_speed <= 0:
                return self.level

            elapsed_minutes = (
                elapsed_seconds / 60.0
            ) * simulation_speed

            # --------------------------------------------
            # Natural drain
            # --------------------------------------------

            natural_drain = max(
                0.0,
                float(
                    cfg.get(
                        "mock_natural_drain_per_minute",
                        0.15,
                    )
                ),
            )

            delta = (
                -natural_drain
                * elapsed_minutes
            )

            # --------------------------------------------
            # Borehole filling
            # --------------------------------------------

            if borehole_active:
                borehole_fill = max(
                    0.0,
                    float(
                        cfg.get(
                            "mock_borehole_fill_per_minute",
                            2.0,
                        )
                    ),
                )

                delta += (
                    borehole_fill
                    * elapsed_minutes
                )

            # --------------------------------------------
            # Irrigation draining
            # --------------------------------------------

            if pump_active:
                irrigation_drain = max(
                    0.0,
                    float(
                        cfg.get(
                            "mock_irrigation_drain_per_minute",
                            1.0,
                        )
                    ),
                )

                delta -= (
                    irrigation_drain
                    * elapsed_minutes
                )

            # --------------------------------------------
            # Apply
            # --------------------------------------------

            self.level += delta

            self.level = max(
                0.0,
                min(
                    100.0,
                    self.level,
                ),
            )

            return self.level

    def pause(self):
        with self.lock:
            self.paused = True
            self.last_update = (
                time.monotonic()
            )

        logger.info(
            "Mock sensor paused"
        )

    def resume(self):
        with self.lock:
            self.paused = False
            self.last_update = (
                time.monotonic()
            )

        logger.info(
            "Mock sensor resumed"
        )

    def set_sensor_error(self, enabled):
        with self.lock:
            self.sensor_error = bool(
                enabled
            )

            self.last_update = (
                time.monotonic()
            )

        logger.warning(
            "Mock sensor error: %s",
            (
                "ENABLED"
                if enabled
                else "DISABLED"
            ),
        )

    def get_status(self):
        with self.lock:
            return {
                "enabled": True,
                "paused": self.paused,
                "sensor_error": self.sensor_error,
                "level": round(
                    self.level,
                    2,
                ),
            }


mock_sensor = MockSensor()


# ============================================================
# MQTT
# ============================================================

mqtt_client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="mina-controller",
)

mqtt_connected = False


def mqtt_publish(
    topic,
    payload,
    retain=False,
):
    global last_mqtt_publish

    if not mqtt_connected:
        return False

    try:
        if not isinstance(
            payload,
            str,
        ):
            payload = json.dumps(
                payload
            )

        result = mqtt_client.publish(
            topic,
            payload,
            qos=1,
            retain=retain,
        )

        if (
            result.rc
            != mqtt.MQTT_ERR_SUCCESS
        ):
            logger.error(
                "MQTT publish failed: "
                "topic=%s rc=%s",
                topic,
                result.rc,
            )

            return False

        last_mqtt_publish = time.time()

        return True

    except Exception:
        logger.exception(
            "MQTT publish error: %s",
            topic,
        )

        return False


def on_connect(
    client,
    userdata,
    flags,
    reason_code,
    properties,
):
    global mqtt_connected

    if reason_code.is_failure:
        mqtt_connected = False

        logger.error(
            "MQTT connection failed: %s",
            reason_code,
        )

        return

    mqtt_connected = True

    logger.info(
        "MQTT connected"
    )

    topics = [
        TOPIC_PUMP_COMMAND,
        TOPIC_BOREHOLE_COMMAND,
        TOPIC_MODE_COMMAND,
        TOPIC_CONFIG_COMMAND,
        TOPIC_SIMULATION_COMMAND,
    ]

    for topic in topics:
        result, _ = client.subscribe(
            topic,
            qos=1,
        )

        if result != mqtt.MQTT_ERR_SUCCESS:
            logger.error(
                "Failed to subscribe to %s: %s",
                topic,
                result,
            )

    publish_state()
    publish_health()
    publish_metrics()

    if MOCK_SENSOR:
        publish_simulation_status()


def on_disconnect(
    client,
    userdata,
    disconnect_flags,
    reason_code,
    properties,
):
    global mqtt_connected

    mqtt_connected = False

    logger.warning(
        "MQTT disconnected: %s",
        reason_code,
    )


def on_message(
    client,
    userdata,
    message,
):
    metrics[
        "mqtt_messages"
    ] += 1

    topic = message.topic

    try:
        payload = message.payload.decode(
            "utf-8"
        ).strip()

        logger.info(
            "MQTT command: %s -> %s",
            topic,
            payload,
        )

        if topic == TOPIC_PUMP_COMMAND:
            handle_pump_command(
                payload
            )

        elif topic == TOPIC_BOREHOLE_COMMAND:
            handle_borehole_command(
                payload
            )

        elif topic == TOPIC_MODE_COMMAND:
            handle_mode_command(
                payload
            )

        elif topic == TOPIC_CONFIG_COMMAND:
            handle_config_command(
                payload
            )

        elif topic == TOPIC_SIMULATION_COMMAND:
            handle_simulation_command(
                payload
            )

    except Exception as exc:
        metrics[
            "control_errors"
        ] += 1

        metrics[
            "last_error"
        ] = str(exc)

        metrics[
            "last_error_at"
        ] = datetime.now().isoformat()

        logger.exception(
            "Failed to process MQTT message"
        )


# ============================================================
# MQTT CONFIG STATUS
# ============================================================

def publish_config_status(
    success,
    error=None,
):
    payload = {
        "success": bool(success),
        "error": error,
        "timestamp": datetime.now().isoformat(),
    }

    mqtt_publish(
        TOPIC_CONFIG_STATUS,
        payload,
        retain=True,
    )


def handle_config_command(payload):
    try:
        data = json.loads(payload)

        if not isinstance(data, dict):
            raise ValueError(
                "Configuration payload must be an object"
            )

        if data.get("action") == "RESET":
            reset_config()
            return

        save_config_changes(data)

    except Exception as exc:
        logger.exception(
            "Configuration command failed"
        )

        publish_config_status(
            success=False,
            error=str(exc),
        )


# ============================================================
# SIMULATION
# ============================================================

def publish_simulation_status():
    status = mock_sensor.get_status()

    status.update(
        {
            "enabled": MOCK_SENSOR,
            "paused": simulation_paused,
            "sensor_error": (
                simulation_sensor_error
            ),
            "timestamp": datetime.now().isoformat(),
        }
    )

    mqtt_publish(
        TOPIC_SIMULATION_STATUS,
        status,
        retain=True,
    )


def handle_simulation_command(payload):
    global simulation_paused
    global simulation_sensor_error

    try:
        data = json.loads(payload)

        if isinstance(
            data,
            str,
        ):
            action = data.upper()
            data = {
                "action": action
            }

        action = str(
            data.get(
                "action",
                "",
            )
        ).upper()

        if action == "PAUSE":
            simulation_paused = True
            mock_sensor.pause()

        elif action == "RESUME":
            simulation_paused = False
            mock_sensor.resume()

        elif action == "RESET":
            simulation_paused = False
            simulation_sensor_error = False

            mock_sensor.reset()

        elif action == "SENSOR_ERROR":
            enabled = bool(
                data.get(
                    "enabled",
                    True,
                )
            )

            simulation_sensor_error = (
                enabled
            )

            mock_sensor.set_sensor_error(
                enabled
            )

        elif action == "SET_LEVEL":
            level = data.get("level")

            if level is None:
                raise ValueError(
                    "SET_LEVEL requires level"
                )

            mock_sensor.set_level(
                float(level)
            )

        elif action == "SPEED":
            speed = data.get("speed")

            if speed is None:
                raise ValueError(
                    "SPEED requires speed"
                )

            speed = float(speed)

            if speed <= 0:
                raise ValueError(
                    "Simulation speed must "
                    "be greater than zero"
                )

            save_config_changes(
                {
                    "mock_simulation_speed": speed
                }
            )

        else:
            raise ValueError(
                f"Unknown simulation action: {action}"
            )

        publish_simulation_status()

    except Exception as exc:
        logger.exception(
            "Simulation command failed"
        )

        publish_simulation_status()


# ============================================================
# OUTPUT CONTROL
# ============================================================

def start_pump():
    global pump_active

    with state_lock:
        if pump_active:
            return

        irrigation_pump.on()

        pump_active = True

        metrics[
            "pump_starts"
        ] += 1

    save_event(
        "PUMP",
        "Irrigation pump started",
    )

    publish_state()


def stop_pump(reason=None):
    global pump_active

    with state_lock:
        if not pump_active:
            return

        irrigation_pump.off()

        pump_active = False

        metrics[
            "pump_stops"
        ] += 1

    save_event(
        "PUMP",
        "Irrigation pump stopped",
        {
            "reason": reason
        }
        if reason
        else None,
    )

    publish_state()

def stop_borehole(reason=None):
    global borehole_active
    global borehole_started_at
    global borehole_reason
    global recovery_started_at

    with state_lock:

        if not borehole_active:
            return

        valve.off()

        borehole_active = False

        borehole_started_at = None
        borehole_reason = None
        recovery_started_at = None

        metrics[
            "borehole_stops"
        ] += 1

    save_event(
        "BOREHOLE",
        "Borehole stopped",
        {
            "reason": reason
        }
        if reason
        else None,
    )

    logger.info(
        "Borehole stopped: %s",
        reason or "unknown",
    )

    publish_state()

def start_borehole(reason="scheduled"):
    global borehole_active
    global borehole_started_at
    global borehole_reason
    global recovery_started_at

    # --------------------------------------------------------
    # Never run borehole and irrigation simultaneously.
    # --------------------------------------------------------

    if pump_active:
        stop_pump(
            reason="borehole start"
        )

    with state_lock:

        if borehole_active:
            return False

        valve.on()

        borehole_active = True

        borehole_started_at = time.time()

        borehole_reason = reason

        if reason == "recovery":
            recovery_started_at = time.time()
        else:
            recovery_started_at = None

        metrics[
            "borehole_starts"
        ] += 1

    save_event(
        "BOREHOLE",
        "Borehole started",
        {
            "reason": reason
        },
    )

    logger.info(
        "Borehole started: %s",
        reason,
    )

    publish_state()

    return True

# ============================================================
# MQTT COMMAND HANDLERS
# ============================================================

def handle_pump_command(payload):
    command = str(
        payload
    ).strip().upper()

    if command == "ON":
        start_pump()

    elif command == "OFF":
        stop_pump(
            reason="MQTT command"
        )

    elif command == "AUTO":
        set_system_mode("AUTO")

    else:
        raise ValueError(
            f"Unknown pump command: {command}"
        )


def handle_borehole_command(payload):
    command = str(
        payload
    ).strip().upper()

    if command == "ON":
        start_borehole(
            reason="manual MQTT command"
        )

    elif command == "OFF":
        stop_borehole(
            reason="MQTT command"
        )

    else:
        raise ValueError(
            f"Unknown borehole command: {command}"
        )


def set_system_mode(mode):
    global system_mode

    mode = str(
        mode
    ).strip().upper()

    valid_modes = {
        "AUTO",
        "MANUAL_ON",
        "MANUAL_OFF",
    }

    if mode not in valid_modes:
        raise ValueError(
            f"Invalid system mode: {mode}"
        )

    with state_lock:
        system_mode = mode

    save_event(
        "MODE",
        "System mode changed",
        {
            "mode": mode
        },
    )

    publish_state()

    logger.info(
        "System mode: %s",
        mode,
    )


def handle_mode_command(payload):
    command = str(
        payload
    ).strip().upper()

    set_system_mode(
        command
    )


# ============================================================
# IRRIGATION CONTROL
# ============================================================

def control_irrigation(level):
    cfg = get_config_snapshot()

    critical_level = (
        cfg["critical_level"]
    )

    irrigation_start = (
        cfg["irrigation_start_level"]
    )

    irrigation_stop = (
        cfg["irrigation_stop_level"]
    )

    # --------------------------------------------------------
    # Critical level
    # --------------------------------------------------------

    if level <= critical_level:
        if pump_active:
            stop_pump(
                reason="critical water level"
            )

        return

    # --------------------------------------------------------
    # Borehole active
    # --------------------------------------------------------

    if borehole_active:
        if pump_active:
            stop_pump(
                reason="borehole active"
            )

        return

    # --------------------------------------------------------
    # Manual OFF
    # --------------------------------------------------------

    if system_mode == "MANUAL_OFF":
        if pump_active:
            stop_pump(
                reason="manual off"
            )

        return

    # --------------------------------------------------------
    # Manual ON
    # --------------------------------------------------------

    if system_mode == "MANUAL_ON":
        if not pump_active:
            start_pump()

        return

    # --------------------------------------------------------
    # AUTO
    # --------------------------------------------------------

    if system_mode == "AUTO":

        if (
            level >= irrigation_start
            and not pump_active
        ):
            start_pump()

        elif (
            level <= irrigation_stop
            and pump_active
        ):
            stop_pump(
                reason="irrigation stop threshold"
            )


# ============================================================
# BOREHOLE CONTROL
# ============================================================

def should_run_scheduled_borehole(level):
    global last_scheduled_run

    cfg = get_config_snapshot()

    now = datetime.now()

    current_hour = now.hour

    schedule = cfg["horas"]

    max_level = (
        cfg["scheduled_borehole_max_level"]
    )

    if current_hour not in schedule:
        return False

    if level >= max_level:
        return False

    schedule_key = now.strftime(
        "%Y-%m-%d-%H"
    )

    if last_scheduled_run == schedule_key:
        return False

    last_scheduled_run = schedule_key

    return True


def control_borehole(level):
    global recovery_started_at

    cfg = get_config_snapshot()

    critical_level = float(
        cfg["critical_level"]
    )

    recovery_target = float(
        cfg["recovery_target"]
    )

    normal_runtime = float(
        cfg["normal_borehole_runtime_minutes"]
    )

    max_runtime = float(
        cfg["max_borehole_runtime_minutes"]
    )

    max_recovery = float(
        cfg["max_recovery_minutes"]
    )

    with state_lock:
        active = borehole_active
        started_at = borehole_started_at
        reason = borehole_reason
        mode = system_mode

    # ---------------------------------------------------------
    # BOREHOLE JÁ ESTÁ LIGADO
    # ---------------------------------------------------------

    if active:

        # Estado inconsistente: válvula ligada mas sem timestamp.
        # Não alteramos o global aqui; usamos um timestamp local.
        if started_at is None:
            logger.warning(
                "Borehole is active but has no start timestamp"
            )

            started_at = time.time()

        runtime_minutes = (
            time.time() - started_at
        ) / 60.0

        # -----------------------------------------------------
        # RECOVERY
        # -----------------------------------------------------

        if reason == "recovery":

            # Atingiu o nível de recuperação
            if level >= recovery_target:

                stop_borehole(
                    reason="recovery target reached"
                )

                return

            # Recovery demorou demasiado
            if runtime_minutes >= max_recovery:

                stop_borehole(
                    reason="maximum recovery runtime"
                )

                return

        # -----------------------------------------------------
        # OPERAÇÃO NORMAL / AGENDADA
        # -----------------------------------------------------

        else:

            # Tempo normal terminou
            if runtime_minutes >= normal_runtime:

                stop_borehole(
                    reason="normal runtime reached"
                )

                return

            # Limite absoluto de segurança
            if runtime_minutes >= max_runtime:

                stop_borehole(
                    reason="maximum borehole runtime"
                )

                return

        return

    # ---------------------------------------------------------
    # BOREHOLE ESTÁ DESLIGADO
    # ---------------------------------------------------------

    # Manual OFF impede qualquer arranque automático
    if mode == "MANUAL_OFF":
        return

    # ---------------------------------------------------------
    # RECOVERY DE NÍVEL CRÍTICO
    # ---------------------------------------------------------

    if level <= critical_level:

        start_borehole(
            reason="recovery"
        )

        return

    # ---------------------------------------------------------
    # FUNCIONAMENTO AUTOMÁTICO AGENDADO
    # ---------------------------------------------------------

    if mode == "AUTO":

        if should_run_scheduled_borehole(level):

            start_borehole(
                reason="scheduled"
            )


# ============================================================
# SENSOR FAILURE
# ============================================================

def handle_sensor_failure(error):
    global sensor_failure_active

    if not sensor_failure_active:
        sensor_failure_active = True

        metrics[
            "sensor_errors"
        ] += 1

        save_event(
            "SENSOR",
            "Water level sensor failure",
            {
                "error": str(error)
            },
        )

        logger.error(
            "Sensor failure: %s",
            error,
        )

    # Safety first.
    stop_pump(
        reason="sensor failure"
    )

    stop_borehole(
        reason="sensor failure"
    )


def clear_sensor_failure():
    global sensor_failure_active

    if sensor_failure_active:
        sensor_failure_active = False

        save_event(
            "SENSOR",
            "Water level sensor recovered",
        )

        logger.info(
            "Sensor recovered"
        )


# ============================================================
# SENSOR READING
# ============================================================

def read_sensor():
    if MOCK_SENSOR:
        return mock_sensor.get_level()

    # --------------------------------------------------------
    # HC-SR04 / ultrasonic sensor
    # --------------------------------------------------------

    try:
        from gpiozero import DistanceSensor

        sensor = DistanceSensor(
            echo=SENSOR_ECHO,
            trigger=SENSOR_TRIGGER,
            max_distance=2.0,
        )

        distance = sensor.distance

        sensor.close()

        cfg = get_config_snapshot()

        fundo = cfg["fundo"]
        limite = cfg["limite"]

        # Distance is measured from the sensor
        # downwards. Convert to water height.
        water_height = (
            fundo - distance
        )

        usable_height = (
            fundo - limite
        )

        if usable_height <= 0:
            raise RuntimeError(
                "Invalid tank dimensions"
            )

        level = (
            water_height
            / usable_height
        ) * 100.0

        return max(
            0.0,
            min(
                100.0,
                level,
            ),
        )

    except Exception:
        raise


def read_stable_level(samples=5):
    readings = []

    for _ in range(samples):
        level = read_sensor()

        if level is None:
            continue

        readings.append(
            float(level)
        )

        if len(readings) < samples:
            time.sleep(0.05)

    if not readings:
        raise RuntimeError(
            "No valid sensor readings"
        )

    return median(readings)


# ============================================================
# STATE PUBLISHING
# ============================================================

def publish_state():
    with state_lock:
        level = current_level
        liters = current_liters
        pump = pump_active
        borehole = borehole_active
        mode = system_mode

    if level is not None:
        mqtt_publish(
            TOPIC_WATER_LEVEL,
            {
                "level": round(
                    float(level),
                    2,
                ),
                "timestamp": datetime.now().isoformat(),
            },
            retain=True,
        )

    if liters is not None:
        mqtt_publish(
            TOPIC_WATER_LITERS,
            {
                "liters": round(
                    float(liters),
                    3,
                ),
                "timestamp": datetime.now().isoformat(),
            },
            retain=True,
        )

    mqtt_publish(
        TOPIC_PUMP_STATUS,
        {
            "active": pump,
            "status": (
                "LIGADA"
                if pump
                else "DESLIGADA"
            ),
            "timestamp": datetime.now().isoformat(),
        },
        retain=True,
    )

    mqtt_publish(
        TOPIC_BOREHOLE_STATUS,
        {
            "active": borehole,
            "status": (
                "LIGADO"
                if borehole
                else "DESLIGADO"
            ),
            "reason": borehole_reason,
            "timestamp": datetime.now().isoformat(),
        },
        retain=True,
    )

    mqtt_publish(
        TOPIC_SYSTEM_MODE,
        {
            "mode": mode,
            "timestamp": datetime.now().isoformat(),
        },
        retain=True,
    )


def publish_health():
    uptime = (
        time.time()
        - controller_started_at
    )

    if sensor_failure_active:
        status = "SENSOR_ERROR"

    elif mqtt_connected:
        status = "ONLINE"

    else:
        status = "OFFLINE"

    payload = {
        "status": status,
        "mqtt_connected": mqtt_connected,
        "uptime_seconds": round(
            uptime,
            2,
        ),
        "timestamp": datetime.now().isoformat(),
    }

    mqtt_publish(
        TOPIC_SYSTEM_HEALTH,
        payload,
        retain=True,
    )


def publish_metrics():
    with state_lock:
        current_state = {
            "level": current_level,
            "liters": current_liters,
            "pump_active": pump_active,
            "borehole_active": borehole_active,
            "mode": system_mode,
        }

    payload = {
        **deepcopy(metrics),
        "state": current_state,
        "timestamp": datetime.now().isoformat(),
    }

    mqtt_publish(
        TOPIC_SYSTEM_METRICS,
        payload,
        retain=True,
    )


# ============================================================
# CONTROL LOOP
# ============================================================

def control_loop():
    global current_level
    global current_liters
    global last_loop_time
    global last_measurement_time
    global last_config_reload
    global last_simulation_publish

    logger.info(
        "Controller loop started"
    )

    while running:

        loop_started = time.monotonic()

        try:
            # ------------------------------------------------
            # Reload configuration periodically.
            # ------------------------------------------------

            now = time.time()

            if (
                now - last_config_reload
                >= CONFIG_RELOAD_INTERVAL_SECONDS
            ):
                try:
                    load_config()

                except Exception:
                    logger.exception(
                        "Configuration reload failed"
                    )

                last_config_reload = now

            # ------------------------------------------------
            # Read sensor
            # ------------------------------------------------

            try:
                level = read_stable_level()

                clear_sensor_failure()

            except Exception as exc:
                handle_sensor_failure(
                    exc
                )

                publish_health()

                time.sleep(
                    LOOP_INTERVAL_SECONDS
                )

                continue

            liters = calculate_liters(
                level
            )

            with state_lock:
                current_level = level
                current_liters = liters

            # ------------------------------------------------
            # Automatic control
            # ------------------------------------------------

            try:
                control_borehole(
                    level
                )

                control_irrigation(
                    level
                )

            except Exception:
                metrics[
                    "control_errors"
                ] += 1

                logger.exception(
                    "Control logic failed"
                )

            # ------------------------------------------------
            # Publish state
            # ------------------------------------------------

            publish_state()

            # ------------------------------------------------
            # Measurements
            # ------------------------------------------------

            cfg = get_config_snapshot()

            measurement_interval = float(
                cfg[
                    "measurement_interval_seconds"
                ]
            )

            now = time.time()

            if (
                now - last_measurement_time
                >= measurement_interval
            ):
                save_measurement(
                    level,
                    liters,
                )

                last_measurement_time = now

            # ------------------------------------------------
            # Simulation status
            # ------------------------------------------------

            if MOCK_SENSOR:
                if (
                    now - last_simulation_publish
                    >= 2
                ):
                    publish_simulation_status()

                    last_simulation_publish = now

            # ------------------------------------------------
            # Health / metrics
            # ------------------------------------------------

            publish_health()
            publish_metrics()

            # ------------------------------------------------
            # Loop metrics
            # ------------------------------------------------

            duration_ms = (
                time.monotonic()
                - loop_started
            ) * 1000.0

            metrics[
                "last_loop_duration_ms"
            ] = round(
                duration_ms,
                2,
            )

            metrics[
                "max_loop_duration_ms"
            ] = max(
                metrics[
                    "max_loop_duration_ms"
                ],
                duration_ms,
            )

            metrics[
                "loop_count"
            ] += 1

            last_loop_time = time.time()

        except Exception as exc:
            metrics[
                "loop_errors"
            ] += 1

            metrics[
                "last_error"
            ] = str(exc)

            metrics[
                "last_error_at"
            ] = datetime.now().isoformat()

            logger.exception(
                "Controller loop error"
            )

        finally:
            elapsed = (
                time.monotonic()
                - loop_started
            )

            sleep_time = max(
                0,
                LOOP_INTERVAL_SECONDS
                - elapsed,
            )

            time.sleep(
                sleep_time
            )


# ============================================================
# MQTT SETUP
# ============================================================

def setup_mqtt():
    global mqtt_client

    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message

    mqtt_client.reconnect_delay_set(
        min_delay=1,
        max_delay=MQTT_RECONNECT_DELAY_SECONDS,
    )

    # --------------------------------------------------------
    # Last Will
    # --------------------------------------------------------

    mqtt_client.will_set(
        TOPIC_SYSTEM_HEALTH,
        json.dumps(
            {
                "status": "OFFLINE",
                "mqtt_connected": False,
                "timestamp": datetime.now().isoformat(),
            }
        ),
        qos=1,
        retain=True,
    )

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

    mqtt_client.loop_start()


# ============================================================
# SAFE SHUTDOWN
# ============================================================

def safe_shutdown():
    global running
    global mqtt_connected

    if not running:
        return

    logger.info(
        "Shutting down controller..."
    )

    running = False

    try:
        stop_pump(
            reason="controller shutdown"
        )

    except Exception:
        logger.exception(
            "Failed to stop pump"
        )

    try:
        stop_borehole(
            reason="controller shutdown"
        )

    except Exception:
        logger.exception(
            "Failed to stop borehole"
        )

    # --------------------------------------------------------
    # Publish offline before disconnecting.
    # --------------------------------------------------------

    try:
        if mqtt_connected:
            mqtt_publish(
                TOPIC_SYSTEM_HEALTH,
                {
                    "status": "OFFLINE",
                    "mqtt_connected": False,
                    "timestamp": datetime.now().isoformat(),
                },
                retain=True,
            )

            time.sleep(0.2)

    except Exception:
        logger.exception(
            "Failed to publish OFFLINE state"
        )

    try:
        mqtt_client.loop_stop()
    except Exception:
        logger.exception(
            "Failed to stop MQTT loop"
        )

    try:
        mqtt_client.disconnect()
    except Exception:
        logger.exception(
            "Failed to disconnect MQTT"
        )

    mqtt_connected = False

    logger.info(
        "Controller shutdown complete"
    )


def signal_handler(
    signum,
    frame,
):
    logger.info(
        "Received signal %s",
        signum,
    )

    safe_shutdown()


# ============================================================
# MAIN
# ============================================================

def main():
    logger.info(
        "Starting Mina Controller"
    )

    logger.info(
        "MQTT: %s:%s",
        MQTT_HOST,
        MQTT_PORT,
    )

    logger.info(
        "DB: %s",
        DB_PATH,
    )

    logger.info(
        "MOCK_SENSOR: %s",
        MOCK_SENSOR,
    )

    signal.signal(
        signal.SIGINT,
        signal_handler,
    )

    signal.signal(
        signal.SIGTERM,
        signal_handler,
    )

    try:
        # ----------------------------------------------------
        # Database
        # ----------------------------------------------------

        init_db()

        # ----------------------------------------------------
        # Configuration
        # ----------------------------------------------------

        load_config()

        # ----------------------------------------------------
        # Sensor
        # ----------------------------------------------------

        if MOCK_SENSOR:
            cfg = get_config_snapshot()

            logger.info(
                "Mock sensor enabled"
            )

            logger.info(
                "Initial simulated level: %.2f%%",
                cfg["mock_initial_level"],
            )

        else:
            logger.info(
                "Real sensor enabled"
            )

        # ----------------------------------------------------
        # MQTT
        # ----------------------------------------------------

        setup_mqtt()

        # ----------------------------------------------------
        # Main control loop
        # ----------------------------------------------------

        control_loop()

    except KeyboardInterrupt:
        logger.info(
            "Keyboard interrupt"
        )

    except Exception:
        logger.exception(
            "Fatal controller error"
        )

    finally:
        safe_shutdown()


if __name__ == "__main__":
    main()
