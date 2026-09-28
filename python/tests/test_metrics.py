import os
import socket
import urllib.request
import unittest
from unittest.mock import patch

from tuba_analysis.metrics import Metrics, start_http_server


class MetricsTests(unittest.TestCase):
    def test_metrics_endpoint(self):
        metrics = Metrics()
        metrics.inc("tuba_analysis_processed_events_total", 2)
        metrics.set_watermark(3, 123.5)
        metrics.set_ready(True)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        server = start_http_server(f"127.0.0.1:{port}", metrics)
        try:
            port = server.server_address[1]
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
                text = response.read().decode()
            self.assertIn("tuba_analysis_processed_events_total 2", text)
            self.assertIn('partition="3"', text)
        finally:
            server.shutdown()
            server.server_close()

    def test_listener_defaults_to_loopback_and_requires_opt_in(self):
        metrics = Metrics()
        with patch.dict(os.environ, {"TUBA_ALLOW_NON_LOOPBACK_LISTEN": ""}):
            with self.assertRaisesRegex(ValueError, "wildcard metrics listener requires"):
                start_http_server(":9090", metrics)
            with self.assertRaisesRegex(ValueError, "non-loopback metrics listener requires"):
                start_http_server("192.0.2.10:9090", metrics)

        with patch.dict(os.environ, {"TUBA_ALLOW_NON_LOOPBACK_LISTEN": "true"}):
            server = start_http_server(":19090", metrics)
            try:
                self.assertEqual(server.server_address[0], "0.0.0.0")
            finally:
                server.shutdown()
                server.server_close()

        with patch.dict(os.environ, {"TUBA_ALLOW_NON_LOOPBACK_LISTEN": "TRUE"}):
            with self.assertRaisesRegex(ValueError, "must be exactly true or false"):
                start_http_server("127.0.0.1:9090", metrics)


if __name__ == "__main__":
    unittest.main()
