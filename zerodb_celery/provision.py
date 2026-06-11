"""Auto-provisioning for ZeroDB projects.

Creates a free ZeroDB project on first use — no signup, no credit card.
Credentials are cached in ~/.zerodb/credentials.json for reuse.
Also ensures the celery_results table exists.
"""

import json
import os
import sys
from pathlib import Path

import requests

ZERODB_API = "https://api.ainative.studio"
CREDENTIALS_PATH = Path.home() / ".zerodb" / "credentials.json"

CELERY_RESULTS_TABLE = "celery_results"
CELERY_RESULTS_COLUMNS = {
    "task_id": "string",
    "status": "string",
    "result": "string",
    "traceback": "string",
    "date_done": "string",
    "meta": "string",
}


def _load_cached_credentials():
    """Load cached credentials from disk."""
    if CREDENTIALS_PATH.exists():
        try:
            data = json.loads(CREDENTIALS_PATH.read_text())
            if data.get("api_key") and data.get("project_id"):
                return data["api_key"], data["project_id"]
        except (json.JSONDecodeError, KeyError):
            pass
    return None, None


def _save_credentials(api_key: str, project_id: str):
    """Cache credentials to disk for reuse."""
    CREDENTIALS_PATH.parent.mkdir(parents=True, exist_ok=True)
    CREDENTIALS_PATH.write_text(json.dumps({
        "api_key": api_key,
        "project_id": project_id,
    }, indent=2))
    try:
        CREDENTIALS_PATH.chmod(0o600)
    except OSError:
        pass


def auto_provision(base_url: str = ZERODB_API) -> tuple:
    """Auto-provision a ZeroDB project.

    Resolution order:
    1. ZERODB_API_KEY + ZERODB_PROJECT_ID env vars
    2. Cached credentials in ~/.zerodb/credentials.json
    3. Provision a new free project via API

    Returns:
        tuple: (api_key, project_id)

    Raises:
        RuntimeError: If provisioning fails.
    """
    # 1. Environment variables
    api_key = os.environ.get("ZERODB_API_KEY")
    project_id = os.environ.get("ZERODB_PROJECT_ID")
    if api_key and project_id:
        return api_key, project_id

    # 2. Cached credentials
    api_key, project_id = _load_cached_credentials()
    if api_key and project_id:
        return api_key, project_id

    # 3. Auto-provision
    print("[zerodb-celery] Auto-provisioning free ZeroDB project...", file=sys.stderr)
    try:
        resp = requests.post(
            f"{base_url}/api/v1/zerodb/projects/provision",
            json={"source": "zerodb-celery"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        api_key = data["api_key"]
        project_id = data["project_id"]
    except requests.RequestException as e:
        raise RuntimeError(
            f"Failed to auto-provision ZeroDB project: {e}\n"
            "Set ZERODB_API_KEY and ZERODB_PROJECT_ID manually, or visit "
            "https://ainative.studio to create a free account."
        ) from e

    _save_credentials(api_key, project_id)

    claim_url = data.get("claim_url", "https://ainative.studio/claim")
    print(
        f"[zerodb-celery] Project provisioned! Claim it at: {claim_url}",
        file=sys.stderr,
    )
    return api_key, project_id


def ensure_results_table(api_key: str, project_id: str, base_url: str = ZERODB_API):
    """Create the celery_results table if it does not exist.

    This is idempotent — calling it multiple times is safe.
    """
    try:
        resp = requests.post(
            f"{base_url}/api/v1/zerodb/tables",
            headers={
                "Authorization": f"Bearer {api_key}",
                "X-Project-ID": project_id,
            },
            json={
                "name": CELERY_RESULTS_TABLE,
                "columns": CELERY_RESULTS_COLUMNS,
            },
            timeout=15,
        )
        # 409 = table already exists — that's fine
        if resp.status_code not in (200, 201, 409):
            resp.raise_for_status()
    except requests.RequestException:
        # Non-fatal: the table might already exist or will be created
        # lazily on first write by ZeroDB
        pass
