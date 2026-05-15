import pytest
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from unittest import mock

from src.pipeline import build_pipeline, run
from src.config import PipelineConfig

# ─── Dummy Transforms to safely replace IO boundaries ──────────────

class DummySource(beam.PTransform):
    # ADDED THIS INIT METHOD to swallow bootstrap_servers, topic, etc.
    def __init__(self, *args, **kwargs):
        pass

    def expand(self, pcoll):
        # Inject one valid trade string to flow through the pipeline
        return pcoll | beam.Create([
            b'{"event_type": "trade", "symbol": "BTCUSDT", "trade_id": 1, "price": "40000", "quantity": "1", "trade_time_ms": 1700000000000, "event_time_ms": 1700000000000}'
        ])

class DummySink(beam.PTransform):
    def __init__(self, *args, **kwargs):
        pass
        
    def expand(self, pcoll):
        # Absorb the output without writing to a real database
        return pcoll | beam.Map(lambda x: x)


# ─── Tests ──────────────────────────────────────────────────────────

@mock.patch("src.pipeline.ReadFromKafka", DummySource)
@mock.patch("src.pipeline.WriteRawTradesToDuckDB", DummySink)
@mock.patch("src.pipeline.WriteDeadLetterToDuckDB", DummySink)
@mock.patch("src.pipeline.WriteToDuckDB", DummySink)
class TestPipelineGraph:

    def test_build_pipeline_graph_constructs_successfully(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_SASL_USERNAME", "user")
        monkeypatch.setenv("KAFKA_SASL_PASSWORD", "pass")
        
        config = PipelineConfig()
        options = PipelineOptions()
        
        # Test that the pipeline builds without dependency or wiring errors
        pipeline = build_pipeline(config, options)
        assert isinstance(pipeline, beam.Pipeline)

    @mock.patch("src.pipeline.build_pipeline")
    def test_run_method_execution(self, mock_build, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        monkeypatch.setenv("KAFKA_SASL_USERNAME", "user")
        monkeypatch.setenv("KAFKA_SASL_PASSWORD", "pass")
        
        # Mock the pipeline object to intercept the .run() command
        mock_pipeline = mock.MagicMock()
        mock_build.return_value = mock_pipeline
        
        run()
        
        # Verify the pipeline was executed and we waited for it to finish
        mock_pipeline.run.assert_called_once()
        mock_pipeline.run.return_value.wait_until_finish.assert_called_once()