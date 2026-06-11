"""ZeroDB Celery Broker — uses ZeroDB event stream as a message queue.

Publishes tasks via POST /api/v1/zerodb/events and consumes them
via GET /api/v1/zerodb/events?topic={queue}.
"""

import json
import time

from kombu.transport import virtual
from kombu.utils.encoding import bytes_to_str

from zerodb_celery.provision import ZERODB_API, auto_provision

import requests


class Channel(virtual.Channel):
    """A Kombu channel backed by ZeroDB event stream."""

    # Prefix for ZeroDB event topics so they don't collide
    _topic_prefix = "celery:"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._api_key = None
        self._project_id = None
        self._base_url = ZERODB_API
        self._provision()

    def _provision(self):
        """Resolve credentials from the broker URL or auto-provision."""
        url = self.connection.client.hostname or ""
        # Parse zerodb://key:project@host or zerodb://auto
        if url and url not in ("auto", "localhost"):
            self._base_url = f"https://{url}"

        transport_opts = self.connection.client.transport_options or {}
        self._api_key = transport_opts.get("api_key")
        self._project_id = transport_opts.get("project_id")
        base = transport_opts.get("base_url")
        if base:
            self._base_url = base

        if not self._api_key or not self._project_id:
            self._api_key, self._project_id = auto_provision(self._base_url)

    def _headers(self):
        return {
            "Authorization": f"Bearer {self._api_key}",
            "X-Project-ID": self._project_id,
            "Content-Type": "application/json",
        }

    def _put(self, queue, message, **kwargs):
        """Publish a message (task) to the queue via ZeroDB events."""
        topic = f"{self._topic_prefix}{queue}"
        payload = message if isinstance(message, str) else json.dumps(message)
        try:
            resp = requests.post(
                f"{self._base_url}/api/v1/zerodb/events",
                headers=self._headers(),
                json={
                    "topic": topic,
                    "data": payload,
                },
                timeout=15,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            raise ChannelError(f"Failed to publish task to ZeroDB: {e}") from e

    def _get(self, queue, timeout=None):
        """Consume the next message from the queue via ZeroDB events."""
        topic = f"{self._topic_prefix}{queue}"
        try:
            resp = requests.get(
                f"{self._base_url}/api/v1/zerodb/events",
                headers=self._headers(),
                params={
                    "topic": topic,
                    "limit": 1,
                    "consume": "true",
                },
                timeout=timeout or 15,
            )
            if resp.status_code == 404:
                raise virtual.Empty()
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException:
            raise virtual.Empty()

        events = data if isinstance(data, list) else data.get("events", [])
        if not events:
            raise virtual.Empty()

        event = events[0]
        raw = event.get("data", event)
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                pass
        return raw

    def _purge(self, queue):
        """Purge all messages from a queue."""
        topic = f"{self._topic_prefix}{queue}"
        try:
            resp = requests.delete(
                f"{self._base_url}/api/v1/zerodb/events",
                headers=self._headers(),
                params={"topic": topic},
                timeout=15,
            )
            if resp.ok:
                return resp.json().get("deleted", 0)
        except requests.RequestException:
            pass
        return 0

    def _size(self, queue):
        """Return approximate queue size."""
        topic = f"{self._topic_prefix}{queue}"
        try:
            resp = requests.get(
                f"{self._base_url}/api/v1/zerodb/events",
                headers=self._headers(),
                params={"topic": topic, "count_only": "true"},
                timeout=10,
            )
            if resp.ok:
                data = resp.json()
                return data.get("count", 0)
        except requests.RequestException:
            pass
        return 0


class ChannelError(Exception):
    """Raised when broker operations fail."""


class Transport(virtual.Transport):
    """Kombu transport that uses ZeroDB as the message broker."""

    Channel = Channel

    driver_type = "zerodb"
    driver_name = "zerodb"

    # Connection defaults
    default_port = 443
    connection_errors = (
        virtual.Transport.connection_errors + (
            requests.ConnectionError,
            requests.Timeout,
        )
    )
    channel_errors = (
        virtual.Transport.channel_errors + (
            ChannelError,
        )
    )

    # Polling interval (seconds) when no messages are available
    polling_interval = 1.0

    def driver_version(self):
        from zerodb_celery import __version__
        return __version__


class ZeroDBBroker:
    """Convenience wrapper for configuring Celery with ZeroDB broker.

    Usage:
        app = Celery('tasks')
        app.config_from_object({
            'broker_url': 'zerodb://auto',
            'broker_transport': 'zerodb_celery.broker:Transport',
        })

    Or use the helper:
        ZeroDBBroker.configure(app)
    """

    TRANSPORT = "zerodb_celery.broker:Transport"

    @classmethod
    def configure(cls, app, api_key=None, project_id=None, base_url=None):
        """Configure a Celery app to use ZeroDB as broker.

        Args:
            app: Celery application instance.
            api_key: ZeroDB API key (auto-provisions if not set).
            project_id: ZeroDB project ID (auto-provisions if not set).
            base_url: ZeroDB API base URL.
        """
        transport_options = {}
        if api_key:
            transport_options["api_key"] = api_key
        if project_id:
            transport_options["project_id"] = project_id
        if base_url:
            transport_options["base_url"] = base_url

        app.conf.update(
            broker_url="zerodb://auto",
            broker_transport=cls.TRANSPORT,
            broker_transport_options=transport_options,
        )
        return app
