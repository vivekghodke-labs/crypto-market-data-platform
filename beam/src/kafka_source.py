"""
Kafka source for Apache Beam (Python SDK).

Uses Beam's official Cross-Language Transform (ReadFromKafka) to spin up a 
Java Expansion Service in the background. This provides robust, native Kafka 
integration rather than a custom Python loop.
"""

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka as OfficialBeamKafkaReader
from .logger import get_logger

logger = get_logger(__name__)


class ReadFromKafka(beam.PTransform):
    """PTransform wrapper for the official Cross-Language Kafka source."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        consumer_group: str,
        sasl_username: str,
        sasl_password: str,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer_group = consumer_group
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password

    def expand(self, pcoll):
        # Configure the Java Kafka Consumer properties
        consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.consumer_group,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'SCRAM-SHA-256',
            # JAAS config is required for SCRAM authentication in the Java worker
            'sasl.jaas.config': f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{self.sasl_username}" password="{self.sasl_password}";',
            'auto.offset.reset': 'earliest',
        }

        return (
            pcoll 
            | "Official Kafka Read" >> OfficialBeamKafkaReader(
                consumer_config=consumer_config,
                topics=[self.topic],
                with_metadata=False
            )
            # OfficialBeamKafkaReader outputs tuples of (key, value).
            # Your original code yielded raw bytes. This Map extracts just the value bytes.
            | "Extract Raw Bytes" >> beam.Map(lambda record: record[1].with_output_types(bytes))
        )