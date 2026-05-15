import os
import pytest
from unittest import mock

from src.config import _get_required_env, _get_env, PipelineConfig, build_pipeline_options

class TestConfig:

    def test_get_required_env_missing(self):
        with pytest.raises(EnvironmentError, match="Required environment variable"):
            _get_required_env("NON_EXISTENT_VAR_123")

    def test_get_required_env_present(self, monkeypatch):
        monkeypatch.setenv("TEST_REQ_VAR", "some_value")
        assert _get_required_env("TEST_REQ_VAR") == "some_value"

    def test_get_env_default(self, monkeypatch):
        monkeypatch.delenv("TEST_OPT_VAR", raising=False)
        assert _get_env("TEST_OPT_VAR", "default_val") == "default_val"

    def test_pipeline_config_local_duckdb(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_SASL_USERNAME", "user")
        monkeypatch.setenv("KAFKA_SASL_PASSWORD", "pass")
        monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
        monkeypatch.setenv("DUCKDB_PATH", "/tmp/local.db")

        config = PipelineConfig()
        assert config.duckdb_path == "/tmp/local.db"
        assert config.kafka_bootstrap_servers == "localhost:9092"
        assert config.kafka_topic == "btc-raw-trades" # default check

    def test_pipeline_config_motherduck(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_SASL_USERNAME", "user")
        monkeypatch.setenv("KAFKA_SASL_PASSWORD", "pass")
        monkeypatch.setenv("MOTHERDUCK_TOKEN", "super_secret_token")

        config = PipelineConfig()
        assert config.duckdb_path == "md:crypto_platform?motherduck_token=super_secret_token"

    def test_build_pipeline_options(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_SASL_USERNAME", "user")
        monkeypatch.setenv("KAFKA_SASL_PASSWORD", "pass")
        
        config = PipelineConfig()
        options = build_pipeline_options(config)
        
        # Options expose a dictionary of all arguments
        opts_dict = options.get_all_options()
        assert opts_dict.get("runner") == "FlinkRunner"
        assert opts_dict.get("streaming") is True