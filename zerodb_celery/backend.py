"""ZeroDB Celery Result Backend — stores task results in a ZeroDB table.

Uses the ZeroDB tables API:
  - POST /api/v1/zerodb/tables/{table}/rows   (store result)
  - GET  /api/v1/zerodb/tables/{table}/rows/{id}  (get result)
"""

import json
import time
from datetime import datetime, timezone

from celery.backends.base import BaseKeyValueStoreBackend
from celery.exceptions import ImproperlyConfigured

import requests

from zerodb_celery.provision import (
    ZERODB_API,
    auto_provision,
    ensure_results_table,
)


class ZeroDBBackend(BaseKeyValueStoreBackend):
    """Celery result backend using ZeroDB tables.

    Configuration via Celery app:
        app.conf.update(
            result_backend='zerodb://auto',
            zerodb_api_key='...',       # optional, auto-provisions
            zerodb_project_id='...',    # optional, auto-provisions
            zerodb_base_url='...',      # optional
            zerodb_results_table='celery_results',  # optional
        )

    Or use the helper:
        ZeroDBBackend.configure(app)
    """

    # Celery uses this to find the backend class from URL scheme
    # e.g., result_backend = 'zerodb://auto'

    def __init__(self, app=None, url=None, **kwargs):
        super().__init__(app=app, url=url, **kwargs)
        self._api_key = None
        self._project_id = None
        self._base_url = ZERODB_API
        self._table = "celery_results"
        self._provisioned = False
        self._resolve_config()

    def _resolve_config(self):
        """Resolve ZeroDB credentials from Celery config or auto-provision."""
        conf = self.app.conf if self.app else None

        if conf:
            self._api_key = getattr(conf, "zerodb_api_key", None)
            self._project_id = getattr(conf, "zerodb_project_id", None)
            base = getattr(conf, "zerodb_base_url", None)
            if base:
                self._base_url = base
            table = getattr(conf, "zerodb_results_table", None)
            if table:
                self._table = table

        if not self._api_key or not self._project_id:
            self._api_key, self._project_id = auto_provision(self._base_url)

    def _ensure_table(self):
        """Lazily ensure the results table exists (once per process)."""
        if not self._provisioned:
            ensure_results_table(self._api_key, self._project_id, self._base_url)
            self._provisioned = True

    def _headers(self):
        return {
            "Authorization": f"Bearer {self._api_key}",
            "X-Project-ID": self._project_id,
            "Content-Type": "application/json",
        }

    def get(self, key):
        """Retrieve a task result by key (task_id)."""
        self._ensure_table()
        try:
            resp = requests.get(
                f"{self._base_url}/api/v1/zerodb/tables/{self._table}/rows/{key}",
                headers=self._headers(),
                timeout=15,
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            row = resp.json()
            # Return the raw stored value (JSON string)
            result_data = row.get("result") or row.get("data", {}).get("result")
            if isinstance(result_data, str):
                return result_data.encode("utf-8")
            return result_data
        except requests.RequestException:
            return None

    def set(self, key, value):
        """Store a task result."""
        self._ensure_table()
        # value comes as bytes from Celery
        if isinstance(value, bytes):
            value = value.decode("utf-8")

        try:
            resp = requests.post(
                f"{self._base_url}/api/v1/zerodb/tables/{self._table}/rows",
                headers=self._headers(),
                json={
                    "id": key,
                    "data": {
                        "task_id": key,
                        "result": value,
                        "date_done": datetime.now(timezone.utc).isoformat(),
                    },
                },
                timeout=15,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            raise BackendError(f"Failed to store result in ZeroDB: {e}") from e

    def mget(self, keys):
        """Retrieve multiple results."""
        return [self.get(key) for key in keys]

    def delete(self, key):
        """Delete a task result."""
        try:
            requests.delete(
                f"{self._base_url}/api/v1/zerodb/tables/{self._table}/rows/{key}",
                headers=self._headers(),
                timeout=15,
            )
        except requests.RequestException:
            pass

    @classmethod
    def configure(cls, app, api_key=None, project_id=None, base_url=None,
                  results_table=None):
        """Configure a Celery app to use ZeroDB as result backend.

        Args:
            app: Celery application instance.
            api_key: ZeroDB API key (auto-provisions if not set).
            project_id: ZeroDB project ID (auto-provisions if not set).
            base_url: ZeroDB API base URL.
            results_table: Name of the results table (default: celery_results).
        """
        conf = {
            "result_backend": "zerodb_celery.backend:ZeroDBBackend",
        }
        if api_key:
            conf["zerodb_api_key"] = api_key
        if project_id:
            conf["zerodb_project_id"] = project_id
        if base_url:
            conf["zerodb_base_url"] = base_url
        if results_table:
            conf["zerodb_results_table"] = results_table

        app.config_from_object(conf)
        return app


class BackendError(Exception):
    """Raised when backend operations fail."""
