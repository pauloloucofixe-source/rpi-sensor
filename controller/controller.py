import json
import logging
import os
import signal
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from statistics import median

import paho.mqtt.client as mqtt
import requests


# ============================================================
# CONFIGURATION
# ============================================================

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

# TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
# TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

DB_PATH = os.getenv("DB_PATH", "/data/mina.db")

MOCK_SENSOR = os.getenv("MOCK_SENSOR", "false").lower() in (
    "1",
    "true",
    "yes",
    "on",
)


# ============================================================
# GPIO CONFIGURATION
# ============================================================

SENSOR_ECHO = 24
SENSOR_TRIGGER = 23

VALVE_GPIO = 17
IRRIGATION_PUMP_GPIO = 27


# ============================================================
# WATER LEVEL CONFIGURATION
# ============================================================

CRITICAL_LEVEL = 10.0
PUMP_MIN_LEVEL = 13.0
RECOVERY_TARGET = 20.0

MAX_RECOVERY_MINUTES = 120
NORMAL_BOREHOLE_RUNTIME_MINUTES = 10
MAX_BOREHOLE_RUNTIME_MINUTES = 30

LOOP_INTERVAL_SECONDS = 2
SENSOR_SAMPLES = 5

MIN_SENSOR_DISTANCE = 0.02
MAX_SENSOR_DISTANCE = 3.0


# ============================================================
# MOCK SENSOR CONFIGURATION
# ============================================================

# Starting water level in percentage.
MOCK_INITIAL_LEVEL = float(os.getenv("MOCK_INITIAL_LEVEL", "50"))

# Natural water consumption / loss.
# Percentage points per minute.
MOCK_NATURAL_DRAIN_PER_MINUTE = float(
    os.getenv("MOCK_NATURAL_DRAIN_PER_MINUTE", "0.15")
)

# Borehole filling rate.
# Percentage points per minute.
MOCK_BOREHOLE_FILL_PER_MINUTE = float(
    os.getenv("MOCK_BOREHOLE_FILL_PER_MINUTE", "2.0")
)

# Irrigation pump draining rate.
# Percentage points per minute.
MOCK_IRRIGATION_DRAIN_PER_MINUTE = float(
    os.getenv("MOCK_IRRIGATION_DRAIN_PER_MINUTE", "1.0")
)


# ============================================================
# MQTT TOPICS
# ============================================================

TOPIC_LEVEL = "mina/water/level"
TOPIC_LITERS = "mina/water/liters"

TOPIC_PUMP_STATUS = "mina/pump/status"
TOPIC_BOREHOLE_STATUS = "mina/borehole/status"

TOPIC_SYSTEM_MODE = "mina/system/mode"
TOPIC_HEALTH = "mina/system/health"

TOPIC_PUMP_COMMAND = "mina/pump/command"
TOPIC_BOREHOLE_COMMAND = "mina/borehole/command"
TOPIC_SYSTEM_MODE_COMMAND = "mina/system/mode/command"


# ============================================================
# DEFAULT PHYSICAL CONFIGURATION
# ============================================================

DEFAULT_CONFIG = {
    "fundo": 0.35,
    "limite": 0.20,
    "largura": 0.15,
    "comprimento": 0.15,
    "horas": [3, 6, 11, 15, 18, 23],
}


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger("mina-controller")


# ============================================================
# GLOBAL STATE
# ============================================================

running = True

state_lock = threading.Lock()

pump_mode = "AUTO"
system_mode = "AUTO"

borehole_recovery = False

borehole_started = None
borehole_until = None
recovery_started = None

last_scheduled_run = None

sensor_valid = True

last_sensor_error_notification = 0.0

last_measurement_save = 0.0

mqtt_client = None

config = DEFAULT_CONFIG.copy()


# ============================================================
# MOCK SENSOR
# ============================================================

class MockDistanceSensor:
    """
    Simulates the ultrasonic sensor.

    The sensor itself exposes a distance from the sensor to
    the water surface, exactly like the real ultrasonic sensor.

    The water level changes according to:
      - natural drainage
      - irrigation pump
      - borehole filling
    """

    def __init__(self, initial_level_percentage=50.0):
        self.level_percentage = max(
            0.0,
            min(100.0, initial_level_percentage),
        )

        self.last_update = time.monotonic()

        logger.warning(
            "MOCK SENSOR ENABLED - no physical GPIO sensor is being used"
        )

    def update(
        self,
        irrigation_pump_on=False,
        borehole_on=False,
    ):
        now = time.monotonic()
        elapsed_seconds = now - self.last_update
        self.last_update = now

        elapsed_minutes = elapsed_seconds / 60.0

        change = -(
            MOCK_NATURAL_DRAIN_PER_MINUTE
            * elapsed_minutes
        )

        if irrigation_pump_on:
            change -= (
                MOCK_IRRIGATION_DRAIN_PER_MINUTE
                * elapsed_minutes
            )

        if borehole_on:
            change += (
                MOCK_BOREHOLE_FILL_PER_MINUTE
                * elapsed_minutes
            )

        self.level_percentage += change

        self.level_percentage = max(
            0.0,
            min(100.0, self.level_percentage),
        )

    @property
    def distance(self):
        """
        Convert simulated percentage into physical distance.

        fundo = sensor reference point
        limite = maximum usable water level
        """

        fundo = float(config["fundo"])
        limite = float(config["limite"])

        max_height = fundo - limite

        water_height = (
            max_height
            * self.level_percentage
            / 100.0
        )

        distance = fundo - water_height

        return max(
            MIN_SENSOR_DISTANCE,
            min(MAX_SENSOR_DISTANCE, distance),
        )

    def close(self):
        logger.info("Mock sensor closed")


# ============================================================
# HARDWARE INITIALIZATION
# ============================================================

sensor = None
valvula_furo = None
bomba_rega = None


def initialize_hardware():
    global sensor
    global valvula_furo
    global bomba_rega

    if MOCK_SENSOR:
        sensor = MockDistanceSensor(
            initial_level_percentage=MOCK_INITIAL_LEVEL
        )

        # Mock outputs.
        # They behave like OutputDevice from the controller's
        # perspective but do not access GPIO.
        valvula_furo = MockOutputDevice("borehole")
        bomba_rega = MockOutputDevice("irrigation")

        return

    logger.info("Initializing real Raspberry Pi GPIO")

    from gpiozero import DistanceSensor, OutputDevice

    sensor = DistanceSensor(
        echo=SENSOR_ECHO,
        trigger=SENSOR_TRIGGER,
        max_distance=3.0,
    )

    valvula_furo = OutputDevice(
        VALVE_GPIO,
        active_high=False,
        initial_value=False,
    )

    bomba_rega = OutputDevice(
        IRRIGATION_PUMP_GPIO,
        active_high=False,
        initial_value=False,
    )

    logger.info("Real GPIO initialized")


class MockOutputDevice:
    def __init__(self, name):
        self.name = name
        self._is_active = False

    def on(self):
        self._is_active = True
        logger.info("MOCK GPIO: %s ON", self.name)

    def off(self):
        self._is_active = False
        logger.info("MOCK GPIO: %s OFF", self.name)

    @property
    def is_active(self):
        return self._is_active

    def close(self):
        self.off()


# ============================================================
# DATABASE
# ============================================================

def get_db():
    conn = sqlite3.connect(
        DB_PATH,
        timeout=10,
        check_same_thread=False,
    )

    conn.row_factory = sqlite3.Row

    return conn


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = get_db()

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS measurements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                level REAL NOT NULL,
                liters REAL NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                type TEXT NOT NULL,
                message TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )

        conn.commit()

    finally:
        conn.close()


def load_config():
    global config

    conn = get_db()

    try:
        rows = conn.execute(
            "SELECT key, value FROM config"
        ).fetchall()

        if not rows:
            config = DEFAULT_CONFIG.copy()

            for key, value in config.items():
                conn.execute(
                    """
                    INSERT OR REPLACE INTO config
                    (key, value)
                    VALUES (?, ?)
                    """,
                    (key, json.dumps(value)),
                )

            conn.commit()

        else:
            config = DEFAULT_CONFIG.copy()

            for row in rows:
                try:
                    config[row["key"]] = json.loads(row["value"])
                except json.JSONDecodeError:
                    logger.warning(
                        "Invalid config value for %s",
                        row["key"],
                    )

    finally:
        conn.close()

    validate_physical_config()

    logger.info("Configuration loaded: %s", config)


def save_config():
    conn = get_db()

    try:
        for key, value in config.items():
            conn.execute(
                """
                INSERT OR REPLACE INTO config
                (key, value)
                VALUES (?, ?)
                """,
                (key, json.dumps(value)),
            )

        conn.commit()

    finally:
        conn.close()


def validate_physical_config():
    fundo = float(config["fundo"])
    limite = float(config["limite"])
    largura = float(config["largura"])
    comprimento = float(config["comprimento"])

    if fundo <= limite:
        raise ValueError(
            "Invalid configuration: fundo must be greater than limite"
        )

    if largura <= 0 or comprimento <= 0:
        raise ValueError(
            "Invalid configuration: dimensions must be positive"
        )

    if not isinstance(config["horas"], list):
        raise ValueError(
            "Invalid configuration: horas must be a list"
        )


def save_measurement(level, liters):
    conn = get_db()

    try:
        conn.execute(
            """
            INSERT INTO measurements
            (timestamp, level, liters)
            VALUES (?, ?, ?)
            """,
            (
                datetime.now().isoformat(),
                level,
                liters,
            ),
        )

        conn.commit()

    finally:
        conn.close()


def save_event(event_type, message):
    conn = get_db()

    try:
        conn.execute(
            """
            INSERT INTO events
            (timestamp, type, message)
            VALUES (?, ?, ?)
            """,
            (
                datetime.now().isoformat(),
                event_type,
                message,
            ),
        )

        conn.commit()

    finally:
        conn.close()


def cleanup_database():
    cutoff = (
        datetime.now()
        - timedelta(days=7)
    ).isoformat()

    conn = get_db()

    try:
        conn.execute(
            "DELETE FROM measurements WHERE timestamp < ?",
            (cutoff,),
        )

        conn.execute(
            "DELETE FROM events WHERE timestamp < ?",
            (cutoff,),
        )

        conn.commit()

    finally:
        conn.close()


# ============================================================
# TELEGRAM
# ============================================================

# def send_telegram(message):
#     if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
#         return

#     url = (
#         f"https://api.telegram.org/bot"
#         f"{TELEGRAM_TOKEN}/sendMessage"
#     )

#     try:
#         requests.post(
#             url,
#             json={
#                 "chat_id": TELEGRAM_CHAT_ID,
#                 "text": message,
#             },
#             timeout=10,
#         )

#     except requests.RequestException as exc:
#         logger.error(
#             "Telegram notification failed: %s",
#             exc,
#         )


def alert(event_type, message):
    logger.warning(
        "[%s] %s",
        event_type,
        message,
    )

    save_event(event_type, message)

    # send_telegram(
    #     f"[MINA] {message}"
    # )


# ============================================================
# MQTT
# ============================================================

def mqtt_publish(topic, payload, retain=False):
    if mqtt_client is None:
        return False

    try:
        if not mqtt_client.is_connected():
            return False

        result = mqtt_client.publish(
            topic,
            str(payload),
            qos=1,
            retain=retain,
        )

        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.warning(
                "MQTT publish failed: topic=%s rc=%s",
                topic,
                result.rc,
            )
            return False

        return True

    except Exception as exc:
        logger.error(
            "MQTT publish exception: %s",
            exc,
        )

        return False


def on_connect(
    client,
    userdata,
    flags,
    reason_code,
    properties,
):
    if reason_code == 0:
        logger.info("Connected to MQTT broker")

        client.subscribe(
            TOPIC_PUMP_COMMAND,
            qos=1,
        )

        client.subscribe(
            TOPIC_BOREHOLE_COMMAND,
            qos=1,
        )

        client.subscribe(
            TOPIC_SYSTEM_MODE_COMMAND,
            qos=1,
        )

        mqtt_publish(
            TOPIC_HEALTH,
            "ONLINE",
            retain=True,
        )

    else:
        logger.error(
            "MQTT connection failed: %s",
            reason_code,
        )


def on_disconnect(
    client,
    userdata,
    disconnect_flags,
    reason_code,
    properties,
):
    logger.warning(
        "Disconnected from MQTT: %s",
        reason_code,
    )


def configure_mqtt():
    global mqtt_client

    mqtt_client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="mina-controller",
    )

    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message

    mqtt_client.will_set(
        TOPIC_HEALTH,
        "OFFLINE",
        qos=1,
        retain=True,
    )


def connect_mqtt():
    while running:
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

            mqtt_client.loop_start()

            return True

        except Exception as exc:
            logger.error(
                "MQTT connection failed: %s",
                exc,
            )

            time.sleep(5)

    return False


def on_message(client, userdata, msg):
    global pump_mode
    global system_mode

    try:
        payload = (
            msg.payload
            .decode("utf-8")
            .strip()
            .upper()
        )

        logger.info(
            "MQTT command: %s -> %s",
            msg.topic,
            payload,
        )

        if msg.topic == TOPIC_PUMP_COMMAND:
            if payload in ("ON", "OFF", "AUTO"):
                pump_mode = payload

                save_event(
                    "PUMP_COMMAND",
                    f"Pump mode changed to {payload}",
                )

        elif msg.topic == TOPIC_BOREHOLE_COMMAND:
            if payload == "ON":
                start_borehole_manual()

            elif payload == "OFF":
                stop_borehole(
                    reason="Manual command"
                )

        elif msg.topic == TOPIC_SYSTEM_MODE_COMMAND:
            if payload in (
                "AUTO",
                "MANUAL_ON",
                "MANUAL_OFF",
            ):
                system_mode = payload

                save_event(
                    "SYSTEM_MODE",
                    f"System mode changed to {payload}",
                )

                if payload == "MANUAL_OFF":
                    safe_all_outputs_off()

    except Exception:
        logger.exception(
            "Error processing MQTT message"
        )


# ============================================================
# SENSOR
# ============================================================

def read_sensor_distance():
    if MOCK_SENSOR:
        sensor.update(
            irrigation_pump_on=bomba_rega.is_active,
            borehole_on=valvula_furo.is_active,
        )

    samples = []

    for _ in range(SENSOR_SAMPLES):
        distance = float(sensor.distance)

        if (
            distance < MIN_SENSOR_DISTANCE
            or distance > MAX_SENSOR_DISTANCE
        ):
            raise ValueError(
                f"Invalid sensor distance: {distance}"
            )

        samples.append(distance)

        if not MOCK_SENSOR:
            time.sleep(0.05)

    return median(samples)


def calculate_water_level(distance):
    fundo = float(config["fundo"])
    limite = float(config["limite"])

    max_height = fundo - limite

    if max_height <= 0:
        raise ValueError(
            "Invalid tank geometry"
        )

    current_height = max(
        0.0,
        fundo - distance,
    )

    percentage = (
        current_height
        / max_height
        * 100
    )

    percentage = max(
        0.0,
        min(100.0, percentage),
    )

    return percentage


def calculate_liters(level_percentage):
    fundo = float(config["fundo"])
    limite = float(config["limite"])

    largura = float(config["largura"])
    comprimento = float(config["comprimento"])

    max_height = fundo - limite

    current_height = (
        max_height
        * level_percentage
        / 100.0
    )

    liters = (
        largura
        * comprimento
        * current_height
        * 1000
    )

    return max(
        0.0,
        liters,
    )


# ============================================================
# OUTPUT CONTROL
# ============================================================

def safe_all_outputs_off():
    global borehole_recovery
    global borehole_started
    global borehole_until
    global recovery_started

    with state_lock:
        bomba_rega.off()
        valvula_furo.off()

        borehole_recovery = False
        borehole_started = None
        borehole_until = None
        recovery_started = None


def start_borehole_manual():
    global borehole_started
    global borehole_until

    if system_mode == "MANUAL_OFF":
        logger.warning(
            "Ignoring borehole ON command because system is MANUAL_OFF"
        )
        return

    if not sensor_valid:
        logger.warning(
            "Ignoring borehole ON command because sensor is invalid"
        )
        return

    with state_lock:
        valvula_furo.on()

        borehole_started = time.monotonic()

        borehole_until = (
            borehole_started
            + NORMAL_BOREHOLE_RUNTIME_MINUTES * 60
        )

    save_event(
        "BOREHOLE",
        "Manual borehole started",
    )


def start_borehole_recovery():
    global borehole_recovery
    global recovery_started
    global borehole_until

    if not sensor_valid:
        return

    if system_mode == "MANUAL_OFF":
        return

    if borehole_recovery:
        return

    now = time.monotonic()

    with state_lock:
        valvula_furo.on()

        borehole_recovery = True
        recovery_started = now

        borehole_until = (
            now
            + MAX_RECOVERY_MINUTES * 60
        )

    alert(
        "RECOVERY",
        (
            "Critical water level detected. "
            "Borehole recovery started."
        ),
    )


def stop_borehole(reason=""):
    global borehole_recovery
    global borehole_started
    global borehole_until
    global recovery_started

    with state_lock:
        valvula_furo.off()

        borehole_recovery = False
        borehole_started = None
        borehole_until = None
        recovery_started = None

    if reason:
        save_event(
            "BOREHOLE",
            f"Borehole stopped: {reason}",
        )


def control_irrigation(level):
    if system_mode == "MANUAL_OFF":
        bomba_rega.off()
        return

    if pump_mode == "MANUAL_OFF":
        bomba_rega.off()
        return

    # Both AUTO and MANUAL_ON are safety-gated by water level.
    if level < PUMP_MIN_LEVEL:
        if bomba_rega.is_active:
            logger.warning(
                "Water level %.2f%% below pump threshold. "
                "Turning irrigation pump OFF.",
                level,
            )

        bomba_rega.off()
        return

    if pump_mode in ("AUTO", "MANUAL_ON"):
        bomba_rega.on()


def control_borehole(
    level,
    now,
):
    global last_scheduled_run
    global borehole_started
    global borehole_until

    if system_mode == "MANUAL_OFF":
        stop_borehole(
            reason="System mode MANUAL_OFF"
        )
        return

    # --------------------------------------------------------
    # Critical level -> recovery
    # --------------------------------------------------------

    if level <= CRITICAL_LEVEL:
        start_borehole_recovery()

    # --------------------------------------------------------
    # Recovery currently running
    # --------------------------------------------------------

    if borehole_recovery:

        if level >= RECOVERY_TARGET:
            stop_borehole(
                reason=(
                    f"Recovery target reached "
                    f"({level:.2f}%)"
                )
            )
            return

        if (
            borehole_until is not None
            and now >= borehole_until
        ):
            stop_borehole(
                reason="Recovery watchdog timeout"
            )

            alert(
                "SAFETY",
                (
                    "Borehole recovery stopped because "
                    "the maximum recovery runtime was reached."
                ),
            )

            return

        return

    # --------------------------------------------------------
    # Existing normal/manual run
    # --------------------------------------------------------

    if borehole_started is not None:

        if (
            borehole_until is not None
            and now >= borehole_until
        ):
            stop_borehole(
                reason="Normal runtime completed"
            )

            return

        # Absolute safety watchdog.
        if (
            now - borehole_started
            >= MAX_BOREHOLE_RUNTIME_MINUTES * 60
        ):
            stop_borehole(
                reason="Hard watchdog timeout"
            )

            alert(
                "SAFETY",
                (
                    "Borehole stopped by hard "
                    "30-minute watchdog."
                ),
            )

            return

        return

    # --------------------------------------------------------
    # Scheduled runs
    # --------------------------------------------------------

    current_datetime = datetime.now()

    current_hour = current_datetime.hour

    scheduled_hours = config.get(
        "horas",
        [],
    )

    schedule_key = (
        current_datetime.date(),
        current_hour,
    )

    if (
        current_hour in scheduled_hours
        and last_scheduled_run != schedule_key
    ):
        last_scheduled_run = schedule_key

        if sensor_valid:
            with state_lock:
                valvula_furo.on()

                borehole_started = now

                borehole_until = (
                    now
                    + NORMAL_BOREHOLE_RUNTIME_MINUTES * 60
                )

            save_event(
                "SCHEDULE",
                (
                    f"Scheduled borehole started "
                    f"at {current_hour}:00"
                ),
            )


# ============================================================
# SENSOR FAILURE
# ============================================================

def handle_sensor_failure(reason):
    global sensor_valid
    global last_sensor_error_notification
    global borehole_recovery
    global borehole_started
    global borehole_until
    global recovery_started

    sensor_valid = False

    # SAFETY FIRST
    bomba_rega.off()
    valvula_furo.off()

    borehole_recovery = False
    borehole_started = None
    borehole_until = None
    recovery_started = None

    now = time.monotonic()

    # Avoid Telegram/event spam every 2 seconds.
    if (
        now - last_sensor_error_notification
        >= 60
    ):
        last_sensor_error_notification = now

        alert(
            "SENSOR_ERROR",
            (
                "Water level sensor failure. "
                "All pumps/valves were turned OFF. "
                f"Reason: {reason}"
            ),
        )


# ============================================================
# STATE PUBLISHING
# ============================================================

def publish_state(
    level,
    liters,
):
    pump_status = (
        "LIGADA"
        if bomba_rega.is_active
        else "DESLIGADA"
    )

    borehole_status = (
        "LIGADO"
        if valvula_furo.is_active
        else "DESLIGADO"
    )

    health = (
        "ONLINE"
        if sensor_valid
        else "SENSOR_ERROR"
    )

    mqtt_publish(
        TOPIC_LEVEL,
        f"{level:.2f}",
        retain=True,
    )

    mqtt_publish(
        TOPIC_LITERS,
        f"{liters:.2f}",
        retain=True,
    )

    mqtt_publish(
        TOPIC_PUMP_STATUS,
        pump_status,
        retain=True,
    )

    mqtt_publish(
        TOPIC_BOREHOLE_STATUS,
        borehole_status,
        retain=True,
    )

    mqtt_publish(
        TOPIC_SYSTEM_MODE,
        system_mode,
        retain=True,
    )

    mqtt_publish(
        TOPIC_HEALTH,
        health,
        retain=True,
    )


# ============================================================
# MAIN LOOP
# ============================================================

def control_loop():
    global sensor_valid
    global last_measurement_save

    while running:
        try:
            distance = read_sensor_distance()

            level = calculate_water_level(
                distance
            )

            liters = calculate_liters(
                level
            )

            if not sensor_valid:
                logger.info(
                    "Water sensor recovered"
                )

                sensor_valid = True

                save_event(
                    "SENSOR",
                    "Water sensor recovered",
                )

                # send_telegram(
                #     "[MINA] Water sensor recovered."
                # )

            now = time.monotonic()

            # --------------------------------------------
            # Control
            # --------------------------------------------

            control_irrigation(level)

            control_borehole(
                level,
                now,
            )

            # --------------------------------------------
            # Publish
            # --------------------------------------------

            publish_state(
                level,
                liters,
            )

            # --------------------------------------------
            # Database
            # --------------------------------------------

            if (
                time.monotonic()
                - last_measurement_save
                >= 900
            ):
                save_measurement(
                    level,
                    liters,
                )

                cleanup_database()

                last_measurement_save = (
                    time.monotonic()
                )

            # --------------------------------------------
            # Logging
            # --------------------------------------------

            logger.info(
                (
                    "Level: %.2f%% | "
                    "Liters: %.2f | "
                    "Pump: %s | "
                    "Borehole: %s | "
                    "Mode: %s%s"
                ),
                level,
                liters,
                (
                    "ON"
                    if bomba_rega.is_active
                    else "OFF"
                ),
                (
                    "ON"
                    if valvula_furo.is_active
                    else "OFF"
                ),
                system_mode,
                (
                    " | MOCK"
                    if MOCK_SENSOR
                    else ""
                ),
            )

        except Exception as exc:
            logger.exception(
                "Controller loop error"
            )

            handle_sensor_failure(
                str(exc)
            )

        time.sleep(
            LOOP_INTERVAL_SECONDS
        )


# ============================================================
# SHUTDOWN
# ============================================================

def shutdown(signum=None, frame=None):
    global running

    if not running:
        return

    logger.warning(
        "Shutting down controller..."
    )

    running = False

    # SAFETY FIRST
    try:
        safe_all_outputs_off()
    except Exception:
        logger.exception(
            "Failed to turn outputs OFF"
        )

    try:
        mqtt_publish(
            TOPIC_HEALTH,
            "OFFLINE",
            retain=True,
        )
    except Exception:
        pass

    try:
        if mqtt_client is not None:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
    except Exception:
        logger.exception(
            "MQTT shutdown failed"
        )

    try:
        if sensor is not None:
            sensor.close()

        if valvula_furo is not None:
            valvula_furo.close()

        if bomba_rega is not None:
            bomba_rega.close()

    except Exception:
        logger.exception(
            "Hardware shutdown failed"
        )

    logger.info(
        "Controller stopped"
    )


# ============================================================
# MAIN
# ============================================================

def main():
    logger.info(
        "Starting Mina Controller"
    )

    logger.info(
        "Mock sensor: %s",
        MOCK_SENSOR,
    )

    init_db()
    load_config()

    initialize_hardware()

    configure_mqtt()

    signal.signal(
        signal.SIGTERM,
        shutdown,
    )

    signal.signal(
        signal.SIGINT,
        shutdown,
    )

    if not connect_mqtt():
        logger.error(
            "Could not establish MQTT connection"
        )

    try:
        control_loop()

    finally:
        shutdown()


if __name__ == "__main__":
    main()