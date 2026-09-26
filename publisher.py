#!/usr/bin/env python3
"""Publish display-ready weather state from VictoriaMetrics to MQTT."""

from __future__ import annotations

import json
import logging
import math
import os
import signal
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import paho.mqtt.client as mqtt
import requests

LOG = logging.getLogger("weather-state-publisher")

WIND_SELECTOR = '{sensor="kulkisegler",__name__=~"wind_speed|wind_direction"}'
TEMPERATURE_SELECTOR = (
    '{__name__="mqtt_consumer_value",'
    'topic=~"wassersensor/sensor/(wasser|luft)temperatur/state"}'
)
DIRECTIONS = (
    "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
    "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
)


@dataclass(frozen=True)
class Sample:
    timestamp: float
    value: float


@dataclass(frozen=True)
class Settings:
    vm_url: str = "http://victoriametrics:8428"
    mqtt_host: str = "mosquitto"
    mqtt_port: int = 1883
    mqtt_username: str | None = None
    mqtt_password: str | None = None
    mqtt_client_id: str = "weather-state-publisher"
    mqtt_wind_topic: str = "kulki/v1/weather/wind"
    mqtt_temperature_topic: str = "kulki/v1/weather/temperatures"
    mqtt_availability_topic: str = "kulki/v1/weather/publisher"
    poll_interval: float = 60.0
    wind_window: float = 600.0
    wind_max_age: float = 360.0
    temperature_max_age: float = 300.0
    query_timeout: float = 10.0
    health_port: int = 8080

    @classmethod
    def from_environment(cls) -> "Settings":
        def optional(name: str) -> str | None:
            value = os.getenv(name)
            return value if value else None

        return cls(
            vm_url=os.getenv("VM_URL", cls.vm_url).rstrip("/"),
            mqtt_host=os.getenv("MQTT_HOST", cls.mqtt_host),
            mqtt_port=int(os.getenv("MQTT_PORT", str(cls.mqtt_port))),
            mqtt_username=optional("MQTT_USERNAME"),
            mqtt_password=optional("MQTT_PASSWORD"),
            mqtt_client_id=os.getenv("MQTT_CLIENT_ID", cls.mqtt_client_id),
            mqtt_wind_topic=os.getenv("MQTT_WIND_TOPIC", cls.mqtt_wind_topic),
            mqtt_temperature_topic=os.getenv(
                "MQTT_TEMPERATURE_TOPIC", cls.mqtt_temperature_topic
            ),
            mqtt_availability_topic=os.getenv(
                "MQTT_AVAILABILITY_TOPIC", cls.mqtt_availability_topic
            ),
            poll_interval=float(
                os.getenv("POLL_INTERVAL_SECONDS", str(cls.poll_interval))
            ),
            wind_window=float(
                os.getenv("WIND_WINDOW_SECONDS", str(cls.wind_window))
            ),
            wind_max_age=float(
                os.getenv("WIND_MAX_AGE_SECONDS", str(cls.wind_max_age))
            ),
            temperature_max_age=float(
                os.getenv(
                    "TEMPERATURE_MAX_AGE_SECONDS",
                    str(cls.temperature_max_age),
                )
            ),
            query_timeout=float(
                os.getenv("QUERY_TIMEOUT_SECONDS", str(cls.query_timeout))
            ),
            health_port=int(os.getenv("HEALTH_PORT", str(cls.health_port))),
        )


class VictoriaMetrics:
    def __init__(self, base_url: str, timeout: float) -> None:
        self.url = f"{base_url}/api/v1/export"
        self.timeout = timeout
        self.session = requests.Session()

    def export(self, selector: str, start: float, end: float) -> dict[tuple[tuple[str, str], ...], list[Sample]]:
        response = self.session.get(
            self.url,
            params={"match[]": selector, "start": start, "end": end},
            timeout=self.timeout,
        )
        response.raise_for_status()

        result: dict[tuple[tuple[str, str], ...], list[Sample]] = {}
        for line in response.iter_lines(decode_unicode=True):
            if not line:
                continue
            item = json.loads(line)
            metric = tuple(sorted(item["metric"].items()))
            samples = result.setdefault(metric, [])
            samples.extend(
                Sample(float(timestamp) / 1000.0, float(value))
                for timestamp, value in zip(item["timestamps"], item["values"])
            )
        for samples in result.values():
            samples.sort(key=lambda sample: sample.timestamp)
        return result


class Health:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.mqtt_connected = False
        self.last_vm_success = 0.0

    def set_mqtt(self, connected: bool) -> None:
        with self._lock:
            self.mqtt_connected = connected

    def vm_succeeded(self) -> None:
        with self._lock:
            self.last_vm_success = time.monotonic()

    def seconds_since_vm_success(self) -> float:
        with self._lock:
            if self.last_vm_success == 0.0:
                return math.inf
            return time.monotonic() - self.last_vm_success

    def ready(self, maximum_query_age: float) -> bool:
        with self._lock:
            return self.mqtt_connected and (
                time.monotonic() - self.last_vm_success <= maximum_query_age
            )


class HealthHandler(BaseHTTPRequestHandler):
    health: Health
    maximum_query_age: float

    def do_GET(self) -> None:  # noqa: N802 - required by BaseHTTPRequestHandler
        if self.path == "/health":
            status, body = 200, b"ok\n"
        elif self.path == "/ready":
            ready = self.health.ready(self.maximum_query_age)
            status, body = (200, b"ready\n") if ready else (503, b"not ready\n")
        else:
            status, body = 404, b"not found\n"
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        LOG.debug("Health request: " + format, *args)


def metric_labels(metric: tuple[tuple[str, str], ...]) -> dict[str, str]:
    return dict(metric)


def find_series(
    series: dict[tuple[tuple[str, str], ...], list[Sample]],
    **labels: str,
) -> list[Sample]:
    for metric, samples in series.items():
        current = metric_labels(metric)
        if all(current.get(key) == value for key, value in labels.items()):
            return samples
    return []


def round_positive(value: float) -> int:
    return int(math.floor(value + 0.5))


def compass_direction(degrees: float) -> str:
    return DIRECTIONS[round_positive((degrees % 360.0) / 22.5) % len(DIRECTIONS)]


def weighted_direction(direction: list[Sample], speed: list[Sample]) -> float | None:
    speeds = {sample.timestamp: sample.value for sample in speed}
    x = 0.0
    y = 0.0
    weight_sum = 0.0
    for sample in direction:
        weight = speeds.get(sample.timestamp, 0.0)
        if weight <= 0.0:
            continue
        radians = math.radians(sample.value)
        x += math.cos(radians) * weight
        y += math.sin(radians) * weight
        weight_sum += weight
    if weight_sum == 0.0 or (x == 0.0 and y == 0.0):
        return None
    return math.degrees(math.atan2(y, x)) % 360.0


def wind_payload(
    series: dict[tuple[tuple[str, str], ...], list[Sample]],
    now: float,
    max_age: float,
) -> dict[str, object] | None:
    speed = find_series(series, __name__="wind_speed", time="instant")
    gust = find_series(series, __name__="wind_speed", time="gust")
    direction = find_series(series, __name__="wind_direction")
    if not speed or not direction:
        return None
    if now - speed[-1].timestamp > max_age or now - direction[-1].timestamp > max_age:
        return None

    speed_knots = sum(sample.value for sample in speed) / len(speed) / 1.852
    result: dict[str, object] = {"speed_kn": round_positive(speed_knots)}

    if gust and now - gust[-1].timestamp <= max_age:
        result["gust_kn"] = round_positive(max(sample.value for sample in gust) / 1.852)

    degrees = weighted_direction(direction, speed)
    if degrees is not None and speed_knots >= 1.0:
        result["direction"] = compass_direction(degrees)
    return result


def temperature_payload(
    series: dict[tuple[tuple[str, str], ...], list[Sample]],
    now: float,
    max_age: float,
) -> dict[str, float] | None:
    result: dict[str, float] = {}
    topics = {
        "water_c": "wassersensor/sensor/wassertemperatur/state",
        "air_c": "wassersensor/sensor/lufttemperatur/state",
    }
    for field, topic in topics.items():
        samples = find_series(series, __name__="mqtt_consumer_value", topic=topic)
        if samples and now - samples[-1].timestamp <= max_age:
            result[field] = round(samples[-1].value, 1)
    return result or None


def encode_payload(payload: dict[str, object] | None) -> bytes:
    if payload is None:
        return b""
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


class Publisher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.vm = VictoriaMetrics(settings.vm_url, settings.query_timeout)
        self.health = Health()
        self.stop_event = threading.Event()
        self.connected_event = threading.Event()
        self.connection_generation = 0
        self.handled_generation = 0
        self.last_payloads: dict[str, bytes] = {}

        self.mqtt = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=settings.mqtt_client_id,
            protocol=mqtt.MQTTv311,
        )
        if settings.mqtt_username:
            self.mqtt.username_pw_set(settings.mqtt_username, settings.mqtt_password)
        self.mqtt.will_set(
            settings.mqtt_availability_topic,
            payload="offline",
            qos=1,
            retain=True,
        )
        self.mqtt.on_connect = self._on_connect
        self.mqtt.on_disconnect = self._on_disconnect

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: object,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        if reason_code.is_failure:
            LOG.error("MQTT connection rejected: %s", reason_code)
            return
        LOG.info("Connected to MQTT broker")
        self.connection_generation += 1
        self.health.set_mqtt(True)
        self.connected_event.set()

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: object,
        disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        LOG.warning("Disconnected from MQTT broker: %s", reason_code)
        self.health.set_mqtt(False)
        self.connected_event.clear()

    def publish(self, topic: str, payload: bytes, force: bool = False) -> None:
        if not force and self.last_payloads.get(topic) == payload:
            return
        info = self.mqtt.publish(topic, payload=payload, qos=1, retain=True)
        info.wait_for_publish(timeout=self.settings.query_timeout)
        if not info.is_published():
            raise RuntimeError(f"MQTT publish to {topic} timed out")
        self.last_payloads[topic] = payload
        LOG.info("Published %s: %s", topic, payload.decode() if payload else "<invalid>")

    def refresh(self, force: bool = False) -> None:
        now = time.time()
        wind_series = self.vm.export(
            WIND_SELECTOR,
            now - self.settings.wind_window,
            now,
        )
        temperature_series = self.vm.export(
            TEMPERATURE_SELECTOR,
            now - self.settings.temperature_max_age,
            now,
        )
        self.health.vm_succeeded()

        self.publish(
            self.settings.mqtt_wind_topic,
            encode_payload(wind_payload(wind_series, now, self.settings.wind_max_age)),
            force,
        )
        self.publish(
            self.settings.mqtt_temperature_topic,
            encode_payload(
                temperature_payload(
                    temperature_series,
                    now,
                    self.settings.temperature_max_age,
                )
            ),
            force,
        )

    def invalidate_data(self) -> None:
        self.publish(self.settings.mqtt_wind_topic, b"")
        self.publish(self.settings.mqtt_temperature_topic, b"")

    def run(self) -> None:
        LOG.info("Connecting to MQTT broker %s:%d", self.settings.mqtt_host, self.settings.mqtt_port)
        self.mqtt.connect_async(self.settings.mqtt_host, self.settings.mqtt_port, keepalive=60)
        self.mqtt.loop_start()
        next_poll = 0.0
        try:
            while not self.stop_event.is_set():
                if not self.connected_event.wait(timeout=1.0):
                    continue
                force = self.handled_generation != self.connection_generation
                if force or time.monotonic() >= next_poll:
                    try:
                        self.refresh(force=force)
                        if force:
                            self.publish(
                                self.settings.mqtt_availability_topic,
                                b"online",
                                force=True,
                            )
                            self.handled_generation = self.connection_generation
                        next_poll = time.monotonic() + self.settings.poll_interval
                    except Exception:
                        LOG.exception("Could not refresh weather state")
                        if self.health.seconds_since_vm_success() > max(
                            self.settings.wind_max_age,
                            self.settings.temperature_max_age,
                        ):
                            try:
                                self.invalidate_data()
                            except Exception:
                                LOG.exception("Could not invalidate stale weather state")
                        next_poll = time.monotonic() + min(self.settings.poll_interval, 15.0)
                self.stop_event.wait(1.0)
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        if self.connected_event.is_set():
            try:
                self.publish(self.settings.mqtt_wind_topic, b"", force=True)
                self.publish(self.settings.mqtt_temperature_topic, b"", force=True)
                self.publish(
                    self.settings.mqtt_availability_topic,
                    b"offline",
                    force=True,
                )
            except Exception:
                LOG.exception("Could not invalidate MQTT state during shutdown")
        self.mqtt.disconnect()
        self.mqtt.loop_stop()


def start_health_server(health: Health, settings: Settings) -> ThreadingHTTPServer:
    handler = type(
        "ConfiguredHealthHandler",
        (HealthHandler,),
        {
            "health": health,
            "maximum_query_age": max(settings.poll_interval * 3, 180.0),
        },
    )
    server = ThreadingHTTPServer(("0.0.0.0", settings.health_port), handler)
    thread = threading.Thread(target=server.serve_forever, name="health", daemon=True)
    thread.start()
    return server


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = Settings.from_environment()
    publisher = Publisher(settings)
    health_server = start_health_server(publisher.health, settings)

    def stop(signum: int, frame: object) -> None:
        LOG.info("Received signal %d", signum)
        publisher.stop_event.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        publisher.run()
    finally:
        health_server.shutdown()
        health_server.server_close()


if __name__ == "__main__":
    main()
