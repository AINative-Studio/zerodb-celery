"""Tests for zerodb-celery backend, broker, and provisioning."""

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
from celery import Celery

from zerodb_celery.provision import (
    _load_cached_credentials,
    _save_credentials,
    auto_provision,
    ensure_results_table,
    CELERY_RESULTS_TABLE,
)
from zerodb_celery.backend import ZeroDBBackend, BackendError
from zerodb_celery.broker import Channel, Transport, ZeroDBBroker, ChannelError


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FAKE_KEY = "zdb_test_key_123"
FAKE_PROJECT = "proj_test_456"
FAKE_URL = "https://fake.zerodb.test"


def _make_celery_app():
    """Create a real Celery app with ZeroDB config for testing."""
    app = Celery("test")
    app.config_from_object({
        "zerodb_api_key": FAKE_KEY,
        "zerodb_project_id": FAKE_PROJECT,
        "zerodb_base_url": FAKE_URL,
        "zerodb_results_table": "celery_results",
        "result_serializer": "json",
        "accept_content": ["json"],
        "result_expires": None,
        "result_backend_transport_options": {},
    })
    return app


# ---------------------------------------------------------------------------
# Provision tests
# ---------------------------------------------------------------------------


class TestProvision:
    def test_load_cached_credentials_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "zerodb_celery.provision.CREDENTIALS_PATH",
            tmp_path / "nonexistent" / "creds.json",
        )
        key, pid = _load_cached_credentials()
        assert key is None
        assert pid is None

    def test_load_cached_credentials_valid(self, tmp_path, monkeypatch):
        creds = tmp_path / "creds.json"
        creds.write_text(json.dumps({
            "api_key": FAKE_KEY,
            "project_id": FAKE_PROJECT,
        }))
        monkeypatch.setattr("zerodb_celery.provision.CREDENTIALS_PATH", creds)
        key, pid = _load_cached_credentials()
        assert key == FAKE_KEY
        assert pid == FAKE_PROJECT

    def test_load_cached_credentials_malformed(self, tmp_path, monkeypatch):
        creds = tmp_path / "creds.json"
        creds.write_text("not json")
        monkeypatch.setattr("zerodb_celery.provision.CREDENTIALS_PATH", creds)
        key, pid = _load_cached_credentials()
        assert key is None
        assert pid is None

    def test_save_credentials(self, tmp_path, monkeypatch):
        creds = tmp_path / ".zerodb" / "credentials.json"
        monkeypatch.setattr("zerodb_celery.provision.CREDENTIALS_PATH", creds)
        _save_credentials("k1", "p1")
        data = json.loads(creds.read_text())
        assert data["api_key"] == "k1"
        assert data["project_id"] == "p1"

    def test_auto_provision_from_env(self, monkeypatch):
        monkeypatch.setenv("ZERODB_API_KEY", FAKE_KEY)
        monkeypatch.setenv("ZERODB_PROJECT_ID", FAKE_PROJECT)
        key, pid = auto_provision()
        assert key == FAKE_KEY
        assert pid == FAKE_PROJECT

    def test_auto_provision_from_cache(self, tmp_path, monkeypatch):
        monkeypatch.delenv("ZERODB_API_KEY", raising=False)
        monkeypatch.delenv("ZERODB_PROJECT_ID", raising=False)
        creds = tmp_path / "creds.json"
        creds.write_text(json.dumps({
            "api_key": "cached_key",
            "project_id": "cached_proj",
        }))
        monkeypatch.setattr("zerodb_celery.provision.CREDENTIALS_PATH", creds)
        key, pid = auto_provision()
        assert key == "cached_key"
        assert pid == "cached_proj"

    @patch("zerodb_celery.provision.requests.post")
    def test_auto_provision_from_api(self, mock_post, tmp_path, monkeypatch):
        monkeypatch.delenv("ZERODB_API_KEY", raising=False)
        monkeypatch.delenv("ZERODB_PROJECT_ID", raising=False)
        monkeypatch.setattr(
            "zerodb_celery.provision.CREDENTIALS_PATH",
            tmp_path / "nonexistent" / "creds.json",
        )
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "api_key": "new_key",
            "project_id": "new_proj",
            "claim_url": "https://ainative.studio/claim/abc",
        }
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        # Need to also patch _save_credentials since path doesn't exist
        monkeypatch.setattr(
            "zerodb_celery.provision.CREDENTIALS_PATH",
            tmp_path / "creds.json",
        )
        key, pid = auto_provision("https://test.api")
        assert key == "new_key"
        assert pid == "new_proj"
        mock_post.assert_called_once()

    @patch("zerodb_celery.provision.requests.post")
    def test_auto_provision_api_failure(self, mock_post, tmp_path, monkeypatch):
        monkeypatch.delenv("ZERODB_API_KEY", raising=False)
        monkeypatch.delenv("ZERODB_PROJECT_ID", raising=False)
        monkeypatch.setattr(
            "zerodb_celery.provision.CREDENTIALS_PATH",
            tmp_path / "nonexistent" / "creds.json",
        )
        import requests as req
        mock_post.side_effect = req.ConnectionError("offline")
        with pytest.raises(RuntimeError, match="Failed to auto-provision"):
            auto_provision("https://test.api")

    @patch("zerodb_celery.provision.requests.post")
    def test_ensure_results_table_created(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp
        # Should not raise
        ensure_results_table(FAKE_KEY, FAKE_PROJECT, FAKE_URL)
        mock_post.assert_called_once()
        call_json = mock_post.call_args.kwargs["json"]
        assert call_json["name"] == CELERY_RESULTS_TABLE

    @patch("zerodb_celery.provision.requests.post")
    def test_ensure_results_table_already_exists(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 409
        mock_post.return_value = mock_resp
        # 409 is fine — table exists
        ensure_results_table(FAKE_KEY, FAKE_PROJECT, FAKE_URL)

    @patch("zerodb_celery.provision.requests.post")
    def test_ensure_results_table_network_error(self, mock_post):
        import requests as req
        mock_post.side_effect = req.ConnectionError("offline")
        # Non-fatal — should not raise
        ensure_results_table(FAKE_KEY, FAKE_PROJECT, FAKE_URL)


# ---------------------------------------------------------------------------
# Backend tests
# ---------------------------------------------------------------------------


class TestBackend:
    @pytest.fixture
    def backend(self, monkeypatch):
        """Create a backend with fake credentials."""
        monkeypatch.setenv("ZERODB_API_KEY", FAKE_KEY)
        monkeypatch.setenv("ZERODB_PROJECT_ID", FAKE_PROJECT)

        app = _make_celery_app()

        with patch("zerodb_celery.backend.auto_provision", return_value=(FAKE_KEY, FAKE_PROJECT)):
            with patch("zerodb_celery.backend.ensure_results_table"):
                be = ZeroDBBackend(app=app)
                be._base_url = FAKE_URL
                be._provisioned = True  # Skip table creation
                return be

    @patch("zerodb_celery.backend.requests.get")
    def test_get_existing_result(self, mock_get, backend):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "task_id": "task-123",
            "result": '{"status": "SUCCESS", "result": 42}',
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = backend.get("task-123")
        assert result is not None

    @patch("zerodb_celery.backend.requests.get")
    def test_get_missing_result(self, mock_get, backend):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_get.return_value = mock_resp

        result = backend.get("nonexistent")
        assert result is None

    @patch("zerodb_celery.backend.requests.get")
    def test_get_network_error(self, mock_get, backend):
        import requests as req
        mock_get.side_effect = req.ConnectionError("offline")
        result = backend.get("task-123")
        assert result is None

    @patch("zerodb_celery.backend.requests.post")
    def test_set_result(self, mock_post, backend):
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        backend.set("task-123", b'{"status": "SUCCESS"}')
        mock_post.assert_called_once()
        call_json = mock_post.call_args.kwargs["json"]
        assert call_json["id"] == "task-123"
        assert call_json["data"]["task_id"] == "task-123"

    @patch("zerodb_celery.backend.requests.post")
    def test_set_result_failure(self, mock_post, backend):
        import requests as req
        mock_post.side_effect = req.ConnectionError("offline")
        with pytest.raises(BackendError, match="Failed to store result"):
            backend.set("task-123", b'{"status": "SUCCESS"}')

    @patch("zerodb_celery.backend.requests.get")
    def test_mget(self, mock_get, backend):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"result": '"done"'}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        results = backend.mget(["t1", "t2"])
        assert len(results) == 2

    @patch("zerodb_celery.backend.requests.delete")
    def test_delete(self, mock_delete, backend):
        mock_resp = MagicMock()
        mock_delete.return_value = mock_resp
        # Should not raise
        backend.delete("task-123")
        mock_delete.assert_called_once()

    def test_configure(self):
        app = MagicMock()
        ZeroDBBackend.configure(
            app,
            api_key="k",
            project_id="p",
            base_url="https://example.com",
            results_table="my_results",
        )
        app.config_from_object.assert_called_once()
        conf = app.config_from_object.call_args[0][0]
        assert conf["zerodb_api_key"] == "k"
        assert conf["zerodb_project_id"] == "p"
        assert conf["zerodb_results_table"] == "my_results"


# ---------------------------------------------------------------------------
# Broker tests
# ---------------------------------------------------------------------------


class TestBroker:
    def test_zero_db_broker_configure(self):
        app = MagicMock()
        app.conf = MagicMock()
        ZeroDBBroker.configure(app, api_key="k", project_id="p")
        app.conf.update.assert_called_once()
        call_kwargs = app.conf.update.call_args.kwargs
        assert call_kwargs["broker_url"] == "zerodb://auto"
        assert call_kwargs["broker_transport"] == ZeroDBBroker.TRANSPORT
        opts = call_kwargs["broker_transport_options"]
        assert opts["api_key"] == "k"
        assert opts["project_id"] == "p"

    def test_zero_db_broker_configure_with_base_url(self):
        app = MagicMock()
        app.conf = MagicMock()
        ZeroDBBroker.configure(app, api_key="k", project_id="p", base_url="https://custom.test")
        opts = app.conf.update.call_args.kwargs["broker_transport_options"]
        assert opts["base_url"] == "https://custom.test"

    def test_transport_driver_version(self):
        transport = Transport.__new__(Transport)
        version = transport.driver_version()
        assert version == "0.1.0"

    def test_transport_driver_type(self):
        assert Transport.driver_type == "zerodb"
        assert Transport.driver_name == "zerodb"

    def test_transport_connection_errors(self):
        import requests as req
        assert req.ConnectionError in Transport.connection_errors
        assert req.Timeout in Transport.connection_errors

    def test_transport_channel_errors(self):
        assert ChannelError in Transport.channel_errors

    def test_channel_error_is_exception(self):
        err = ChannelError("test")
        assert isinstance(err, Exception)
        assert str(err) == "test"


class TestChannelMethods:
    """Test Channel methods by constructing a channel with mocked internals."""

    @pytest.fixture
    def channel(self, monkeypatch):
        """Create a Channel with mocked connection."""
        monkeypatch.setenv("ZERODB_API_KEY", FAKE_KEY)
        monkeypatch.setenv("ZERODB_PROJECT_ID", FAKE_PROJECT)

        ch = Channel.__new__(Channel)
        ch._api_key = FAKE_KEY
        ch._project_id = FAKE_PROJECT
        ch._base_url = FAKE_URL
        ch._topic_prefix = "celery:"
        return ch

    def test_headers(self, channel):
        headers = channel._headers()
        assert headers["Authorization"] == f"Bearer {FAKE_KEY}"
        assert headers["X-Project-ID"] == FAKE_PROJECT
        assert headers["Content-Type"] == "application/json"

    @patch("zerodb_celery.broker.requests.post")
    def test_put_success(self, mock_post, channel):
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        channel._put("default", {"body": "task_data"})
        mock_post.assert_called_once()
        call_json = mock_post.call_args.kwargs["json"]
        assert call_json["topic"] == "celery:default"
        assert "task_data" in call_json["data"]

    @patch("zerodb_celery.broker.requests.post")
    def test_put_string_message(self, mock_post, channel):
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        channel._put("default", "raw_string_message")
        call_json = mock_post.call_args.kwargs["json"]
        assert call_json["data"] == "raw_string_message"

    @patch("zerodb_celery.broker.requests.post")
    def test_put_failure_raises(self, mock_post, channel):
        import requests as req
        mock_post.side_effect = req.ConnectionError("offline")
        with pytest.raises(ChannelError, match="Failed to publish"):
            channel._put("default", {"body": "data"})

    @patch("zerodb_celery.broker.requests.get")
    def test_get_success(self, mock_get, channel):
        from kombu.transport import virtual
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "events": [{"data": '{"body": "task_payload"}'}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = channel._get("default")
        assert result == {"body": "task_payload"}

    @patch("zerodb_celery.broker.requests.get")
    def test_get_list_response(self, mock_get, channel):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [{"data": '{"body": "data"}'}]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = channel._get("default")
        assert result == {"body": "data"}

    @patch("zerodb_celery.broker.requests.get")
    def test_get_empty_raises(self, mock_get, channel):
        from kombu.transport import virtual
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"events": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        with pytest.raises(virtual.Empty):
            channel._get("default")

    @patch("zerodb_celery.broker.requests.get")
    def test_get_404_raises_empty(self, mock_get, channel):
        from kombu.transport import virtual
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_get.return_value = mock_resp

        with pytest.raises(virtual.Empty):
            channel._get("default")

    @patch("zerodb_celery.broker.requests.get")
    def test_get_network_error_raises_empty(self, mock_get, channel):
        from kombu.transport import virtual
        import requests as req
        mock_get.side_effect = req.ConnectionError("offline")

        with pytest.raises(virtual.Empty):
            channel._get("default")

    @patch("zerodb_celery.broker.requests.delete")
    def test_purge_success(self, mock_delete, channel):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {"deleted": 5}
        mock_delete.return_value = mock_resp

        count = channel._purge("default")
        assert count == 5

    @patch("zerodb_celery.broker.requests.delete")
    def test_purge_failure(self, mock_delete, channel):
        import requests as req
        mock_delete.side_effect = req.ConnectionError("offline")
        count = channel._purge("default")
        assert count == 0

    @patch("zerodb_celery.broker.requests.get")
    def test_size_success(self, mock_get, channel):
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {"count": 10}
        mock_get.return_value = mock_resp

        size = channel._size("default")
        assert size == 10

    @patch("zerodb_celery.broker.requests.get")
    def test_size_failure(self, mock_get, channel):
        import requests as req
        mock_get.side_effect = req.ConnectionError("offline")
        size = channel._size("default")
        assert size == 0

    @patch("zerodb_celery.broker.requests.get")
    def test_get_non_json_data(self, mock_get, channel):
        """Test _get when data is not JSON-parseable."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"events": [{"data": "plain text"}]}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = channel._get("default")
        assert result == "plain text"

    def test_provision_from_transport_opts(self, monkeypatch):
        """Test _provision reading from transport_options."""
        ch = Channel.__new__(Channel)
        ch._api_key = None
        ch._project_id = None
        ch._base_url = "https://api.ainative.studio"

        mock_conn = MagicMock()
        mock_conn.client.hostname = "auto"
        mock_conn.client.transport_options = {
            "api_key": "opt_key",
            "project_id": "opt_proj",
            "base_url": "https://custom.url",
        }
        ch.connection = mock_conn

        ch._provision()
        assert ch._api_key == "opt_key"
        assert ch._project_id == "opt_proj"
        assert ch._base_url == "https://custom.url"

    def test_provision_custom_hostname(self, monkeypatch):
        """Test _provision with custom hostname."""
        ch = Channel.__new__(Channel)
        ch._api_key = None
        ch._project_id = None
        ch._base_url = "https://api.ainative.studio"

        mock_conn = MagicMock()
        mock_conn.client.hostname = "my-zerodb.example.com"
        mock_conn.client.transport_options = {
            "api_key": "k",
            "project_id": "p",
        }
        ch.connection = mock_conn

        ch._provision()
        assert ch._base_url == "https://my-zerodb.example.com"

    @patch("zerodb_celery.broker.auto_provision", return_value=("auto_k", "auto_p"))
    def test_provision_auto(self, mock_prov):
        """Test _provision falls back to auto_provision."""
        ch = Channel.__new__(Channel)
        ch._api_key = None
        ch._project_id = None
        ch._base_url = "https://api.ainative.studio"

        mock_conn = MagicMock()
        mock_conn.client.hostname = "auto"
        mock_conn.client.transport_options = {}
        ch.connection = mock_conn

        ch._provision()
        assert ch._api_key == "auto_k"
        assert ch._project_id == "auto_p"
        mock_prov.assert_called_once()


# ---------------------------------------------------------------------------
# Integration-style tests (mocked HTTP)
# ---------------------------------------------------------------------------


class TestIntegration:
    @patch("zerodb_celery.backend.requests.post")
    @patch("zerodb_celery.backend.requests.get")
    def test_store_and_retrieve_roundtrip(self, mock_get, mock_post, monkeypatch):
        """Verify store then retrieve returns the same data."""
        monkeypatch.setenv("ZERODB_API_KEY", FAKE_KEY)
        monkeypatch.setenv("ZERODB_PROJECT_ID", FAKE_PROJECT)

        app = _make_celery_app()

        # Mock post (store)
        post_resp = MagicMock()
        post_resp.raise_for_status = MagicMock()
        post_resp.status_code = 201
        mock_post.return_value = post_resp

        # Mock get (retrieve)
        payload = '{"status": "SUCCESS", "result": [1,2,3]}'
        get_resp = MagicMock()
        get_resp.status_code = 200
        get_resp.json.return_value = {"task_id": "t-1", "result": payload}
        get_resp.raise_for_status = MagicMock()
        mock_get.return_value = get_resp

        with patch("zerodb_celery.backend.auto_provision", return_value=(FAKE_KEY, FAKE_PROJECT)):
            with patch("zerodb_celery.backend.ensure_results_table"):
                be = ZeroDBBackend(app=app)
                be._base_url = FAKE_URL
                be._provisioned = True

        be.set("t-1", payload.encode("utf-8"))
        result = be.get("t-1")
        assert result is not None
        decoded = result.decode("utf-8") if isinstance(result, bytes) else result
        assert "SUCCESS" in decoded

    def test_backend_error_is_exception(self):
        err = BackendError("test error")
        assert isinstance(err, Exception)
        assert str(err) == "test error"
