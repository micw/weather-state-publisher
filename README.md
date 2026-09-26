# weather-state-publisher

Publishes display-ready, retained MQTT weather state derived exclusively from
VictoriaMetrics. Collection and presentation stay independent; the local
temperature display remains the only direct sensor-to-display path.

## Output topics

- `kulki/v1/weather/wind`
- `kulki/v1/weather/temperatures`
- `kulki/v1/weather/publisher` (`online`/`offline` availability)

Example payloads:

```json
{"direction":"WSW","gust_kn":13,"speed_kn":8}
```

```json
{"air_c":21.3,"water_c":18.7}
```

Data topics use QoS 1 and retained messages. A zero-length retained payload
invalidates stale state. The availability topic uses a retained MQTT Last Will.

Wind is calculated from the last ten minutes of VictoriaMetrics samples:

- rounded mean of `wind_speed{time="instant"}` in knots
- rounded maximum of `wind_speed{time="gust"}` in knots
- speed-weighted circular mean mapped to one of 16 compass directions

Temperatures use the latest fresh values from the existing water-sensor MQTT
series in VictoriaMetrics. Identical normalized payloads are not republished.

## Configuration

| Environment variable | Default |
| --- | --- |
| `VM_URL` | `http://victoriametrics:8428` |
| `MQTT_HOST` | `mosquitto` |
| `MQTT_PORT` | `1883` |
| `MQTT_USERNAME` | unset |
| `MQTT_PASSWORD` | unset |
| `MQTT_CLIENT_ID` | `weather-state-publisher` |
| `MQTT_WIND_TOPIC` | `kulki/v1/weather/wind` |
| `MQTT_TEMPERATURE_TOPIC` | `kulki/v1/weather/temperatures` |
| `MQTT_AVAILABILITY_TOPIC` | `kulki/v1/weather/publisher` |
| `POLL_INTERVAL_SECONDS` | `60` |
| `WIND_WINDOW_SECONDS` | `600` |
| `WIND_MAX_AGE_SECONDS` | `360` |
| `TEMPERATURE_MAX_AGE_SECONDS` | `300` |
| `QUERY_TIMEOUT_SECONDS` | `10` |
| `HEALTH_PORT` | `8080` |
| `LOG_LEVEL` | `INFO` |

Endpoints:

- `GET /health`: process is running
- `GET /ready`: MQTT is connected and VictoriaMetrics was queried recently

## Development

```bash
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Build locally:

```bash
docker build -t weather-state-publisher .
```
