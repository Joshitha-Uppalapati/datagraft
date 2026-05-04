from fastapi.testclient import TestClient
from app.main import app


client = TestClient(app)


def test_health_endpoint():
    """
    Basic sanity check.
    If this fails, the app isn't even booting correctly.
    """
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_history_endpoint():
    """
    Ensures DB wiring + router registration is intact.
    We don't care about data yet — just that it doesn't crash.
    """
    response = client.get("/api/history")

    assert response.status_code == 200
    assert isinstance(response.json(), list)