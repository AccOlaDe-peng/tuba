import importlib.util
import os
import unittest


MODULE_PATH = os.path.join(os.path.dirname(__file__), "gen248.py")
SPEC = importlib.util.spec_from_file_location("gen248", MODULE_PATH)
GEN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GEN)


def make_service(name, env):
    return {"command": "/opt/tuba/bin/" + name, "env": dict(env), "chain": "zeek", "pid": 1000}


def base_env(**overrides):
    env = {
        "DATABASE_URL": "postgres://shared",
        "ES_API_KEY": "shared-es-key",
        "KAFKA_SASL_PASSWORD": "placeholder",
        "KAFKA_BROKERS": "kafka:9093",
        "TUBA_NAMESPACE": "zeek",
        "ES_URL": "http://es:9200",
        "PWD": "/root",
        "HOME": "/root",
    }
    env.update(overrides)
    return env


def minimal_services():
    ingest = make_service(
        "zeek-ingest",
        base_env(KAFKA_SASL_PASSWORD="ingest-secret", HTTP_LISTEN="0.0.0.0:9101"),
    )
    indexer = make_service(
        "zeek-raw-indexer",
        base_env(KAFKA_SASL_PASSWORD="indexer-secret", METRICS_LISTEN=":9102"),
    )
    return {"zeek-ingest": ingest, "zeek-raw-indexer": indexer}


class BuildHappyPathTests(unittest.TestCase):
    def test_minimal_two_service_fixture(self):
        file_env, manifest_services = GEN.build(minimal_services())

        self.assertEqual([svc["name"] for svc in manifest_services],
                         ["zeek-ingest", "zeek-raw-indexer"])
        ingest = manifest_services[0]
        indexer = manifest_services[1]

        # Wildcard listeners are dropped, not reproduced.
        self.assertNotIn("HTTP_LISTEN", ingest["environment"])
        self.assertNotIn("METRICS_LISTEN", indexer["environment"])
        self.assertEqual(sorted(GEN.dropped_listeners),
                         ["zeek-ingest.HTTP_LISTEN=0.0.0.0:9101",
                          "zeek-raw-indexer.METRICS_LISTEN=:9102"])

        # Per-service secrets become ${TUBA_<NAME>_KAFKA_SASL_PASSWORD}.
        self.assertEqual(ingest["environment"]["KAFKA_SASL_PASSWORD"],
                         "${TUBA_ZEEK_INGEST_KAFKA_SASL_PASSWORD}")
        self.assertEqual(file_env["TUBA_ZEEK_INGEST_KAFKA_SASL_PASSWORD"], "ingest-secret")
        self.assertEqual(file_env["TUBA_ZEEK_RAW_INDEXER_KAFKA_SASL_PASSWORD"], "indexer-secret")

        # Shared secrets are ${VAR} references landing once in file_env.
        self.assertEqual(ingest["environment"]["DATABASE_URL"], "${DATABASE_URL}")
        self.assertEqual(indexer["environment"]["DATABASE_URL"], "${DATABASE_URL}")
        self.assertEqual(file_env["DATABASE_URL"], "postgres://shared")

        # Ordinary live variables are reproduced verbatim.
        self.assertEqual(ingest["environment"]["ES_URL"], "http://es:9200")


class SharedSecretGuardTests(unittest.TestCase):
    def test_conflicting_shared_secret_values_refuses_write(self):
        services = minimal_services()
        services["zeek-raw-indexer"]["env"]["DATABASE_URL"] = "postgres://other"
        with self.assertRaises(SystemExit):
            GEN.build(services)

    def test_consistent_shared_secret_lands_once(self):
        file_env, manifest_services = GEN.build(minimal_services())
        self.assertNotIn("SOURCE_ADAPTER_TOKEN", file_env)  # absent when no service has it
        self.assertEqual(file_env["DATABASE_URL"], "postgres://shared")
        for svc in manifest_services:
            self.assertEqual(svc["environment"]["DATABASE_URL"], "${DATABASE_URL}")


class FailClosedGuardTests(unittest.TestCase):
    def test_live_variable_reaching_no_service_refuses_write(self):
        # Mutation case: a variable dropped by an over-broad AMBIENT_DENY (the
        # copy loop) but absent from MAY_DROP must trip the guard. Patch the
        # module copies the way such a mutation would.
        original_deny = GEN.AMBIENT_DENY
        GEN.AMBIENT_DENY = original_deny | {"ES_URL"}
        self.addCleanup(setattr, GEN, "AMBIENT_DENY", original_deny)
        with self.assertRaises(SystemExit):
            GEN.build(minimal_services())

    def test_may_drop_variables_do_not_trip_guard(self):
        services = minimal_services()
        # PWD/HOME are in AMBIENT_DENY and MAY_DROP: dropping them is fine.
        file_env, manifest_services = GEN.build(services)
        for svc in manifest_services:
            self.assertNotIn("PWD", svc["environment"])
            self.assertNotIn("HOME", svc["environment"])

    def test_guard_and_copy_loop_name_filters_stay_different(self):
        self.assertNotEqual(GEN.ENV_NAME.pattern, GEN.GUARD_NAME.pattern)

    def test_may_drop_is_a_second_copy_not_an_alias(self):
        self.assertIsNot(GEN.MAY_DROP, GEN.AMBIENT_DENY)


if __name__ == "__main__":
    unittest.main()
