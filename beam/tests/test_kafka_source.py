import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from unittest import mock

from src.kafka_source import ReadFromKafka

class DummyKafkaReader(beam.PTransform):
    """Mocks the Java Cross-Language Kafka reader output format."""
    def __init__(self, *args, **kwargs):
        pass
        
    def expand(self, pcoll):
        # Beam's Official Kafka Reader yields (key, value) pairs.
        return pcoll | beam.Create([
            (b"key1", b"val1"),
            (b"key2", None),      # Simulating a tombstone record (should be filtered out)
            (b"key3", b"val3")
        ])

class TestKafkaSource:

    @mock.patch("src.kafka_source.OfficialBeamKafkaReader", DummyKafkaReader)
    def test_kafka_source_transforms(self):
        with TestPipeline() as p:
            result = p | ReadFromKafka(
                bootstrap_servers="localhost:9092",
                topic="test-topic",
                consumer_group="group",
                sasl_username="user",
                sasl_password="password"
            )
            
            # Assert that the transform extracts the value, drops Nones, and preserves bytes
            assert_that(result, equal_to([b"val1", b"val3"]))