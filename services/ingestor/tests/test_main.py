import os
import asyncio
import pytest
from unittest.mock import patch, AsyncMock, PropertyMock
from fastapi.testclient import TestClient

# INJECT ENVIRONMENT VARIABLES BEFORE IMPORTING THE APP
os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
os.environ["KAFKA_SASL_USERNAME"] = "dummy_user"
os.environ["KAFKA_SASL_PASSWORD"] = "dummy_password"

from src.main import app

@pytest.fixture(autouse=True)
def mock_background_services():
    """
    Mocks the Kafka publisher and WebSocket client so the FastAPI lifespan
    doesn't attempt to make real network connections during the tests.
    """
    with patch("src.main.KafkaPublisher") as mock_pub, \
         patch("src.main.BinanceWebSocketClient") as mock_ws:
        
        mock_ws_instance = mock_ws.return_value
        
        type(mock_ws_instance).stats = PropertyMock(return_value={
            "messages_processed": 0,
            "messages_rejected": 0,
            "reconnect_attempts": 0,
            "running": True
        })
        
        # We simulate a long-running connection that exits cleanly when cancelled.
        async def hanging_run():
            try:
                # Sleep a long time to keep ws_task.done() == False during the test
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                # Catch cancellation triggered by lifespan shutdown
                pass

        mock_ws_instance.run = AsyncMock(side_effect=hanging_run)
        mock_ws_instance.stop = AsyncMock()
        
        yield mock_pub, mock_ws


class TestPingEndpoint:
    
    def test_ping_returns_200(self) -> None:
        with TestClient(app) as client:
            response = client.get("/ping")
            assert response.status_code == 200
            assert response.json() == {"status": "ok"}


class TestHealthEndpoint:
    
    def test_health_returns_200(self) -> None:
        with TestClient(app) as client:
            response = client.get("/health")
            
            # Assertions to ensure it returns 200 Healthy and successfully serialized stats
            assert response.status_code == 200
            
            data = response.json()
            assert data["status"] == "healthy"
            assert data["backend"] == "redpanda-cloud"
            assert "stats" in data
            assert data["stats"]["running"] is True