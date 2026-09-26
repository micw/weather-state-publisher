import unittest

from publisher import (
    Sample,
    compass_direction,
    encode_payload,
    temperature_payload,
    weighted_direction,
    wind_payload,
)


def series(*items):
    result = {}
    for labels, samples in items:
        result[tuple(sorted(labels.items()))] = samples
    return result


class DirectionTests(unittest.TestCase):
    def test_compass_direction_wraps_north(self):
        self.assertEqual(compass_direction(359), "N")
        self.assertEqual(compass_direction(0), "N")
        self.assertEqual(compass_direction(12), "NNE")
        self.assertEqual(compass_direction(247), "WSW")

    def test_weighted_direction_handles_north_boundary(self):
        direction = [Sample(1, 359), Sample(2, 1)]
        speed = [Sample(1, 10), Sample(2, 10)]
        result = weighted_direction(direction, speed)
        self.assertIsNotNone(result)
        self.assertTrue(result < 0.01 or result > 359.99)

    def test_weighted_direction_ignores_calm_samples(self):
        direction = [Sample(1, 90), Sample(2, 270)]
        speed = [Sample(1, 10), Sample(2, 0)]
        self.assertAlmostEqual(weighted_direction(direction, speed), 90)


class PayloadTests(unittest.TestCase):
    def test_wind_payload_uses_average_speed_and_maximum_gust(self):
        now = 1_000.0
        data = series(
            (
                {"__name__": "wind_speed", "sensor": "kulkisegler", "time": "instant"},
                [Sample(900, 9.26), Sample(990, 18.52)],
            ),
            (
                {"__name__": "wind_speed", "sensor": "kulkisegler", "time": "gust"},
                [Sample(900, 18.52), Sample(990, 27.78)],
            ),
            (
                {"__name__": "wind_direction", "sensor": "kulkisegler"},
                [Sample(900, 225), Sample(990, 225)],
            ),
        )
        self.assertEqual(
            wind_payload(data, now, max_age=360),
            {"speed_kn": 8, "gust_kn": 15, "direction": "SW"},
        )

    def test_wind_payload_is_invalid_when_latest_sample_is_stale(self):
        data = series(
            (
                {"__name__": "wind_speed", "time": "instant"},
                [Sample(100, 10)],
            ),
            (
                {"__name__": "wind_direction"},
                [Sample(100, 180)],
            ),
        )
        self.assertIsNone(wind_payload(data, now=500, max_age=360))

    def test_calm_wind_omits_direction(self):
        data = series(
            (
                {"__name__": "wind_speed", "time": "instant"},
                [Sample(100, 0.5)],
            ),
            (
                {"__name__": "wind_direction"},
                [Sample(100, 180)],
            ),
        )
        self.assertEqual(wind_payload(data, now=100, max_age=360), {"speed_kn": 0})

    def test_temperature_payload_keeps_fresh_sensor_independently(self):
        data = series(
            (
                {
                    "__name__": "mqtt_consumer_value",
                    "topic": "wassersensor/sensor/wassertemperatur/state",
                },
                [Sample(990, 18.74)],
            ),
            (
                {
                    "__name__": "mqtt_consumer_value",
                    "topic": "wassersensor/sensor/lufttemperatur/state",
                },
                [Sample(500, 21.26)],
            ),
        )
        self.assertEqual(
            temperature_payload(data, now=1_000, max_age=300),
            {"water_c": 18.7},
        )

    def test_invalid_state_is_an_empty_retained_payload(self):
        self.assertEqual(encode_payload(None), b"")
        self.assertEqual(
            encode_payload({"water_c": 18.7, "air_c": 21.3}),
            b'{"air_c":21.3,"water_c":18.7}',
        )


if __name__ == "__main__":
    unittest.main()
