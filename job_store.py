import base64
import io
import json
from typing import Any, Dict, Optional

import redis


class JobStore:
    """Persist job information in Redis."""

    def __init__(self, url: str = "redis://localhost:6379/0") -> None:
        self._redis = redis.from_url(url)

    def create_job(self, job_id: str, total: int) -> None:
        data = {
            "total": total,
            "index": 0,
            "stage": "queued",
            "file": None,
            "done": False,
            "error": None,
            "zip_bytes": None,
        }
        self._redis.set(job_id, json.dumps(data))

    def update_job(self, job_id: str, **fields: Any) -> None:
        data = self.get_job(job_id) or {}
        zb = fields.get("zip_bytes")
        if isinstance(zb, io.BytesIO):
            fields["zip_bytes"] = base64.b64encode(zb.getvalue()).decode()
        data.update(fields)
        self._redis.set(job_id, json.dumps(data))

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        raw = self._redis.get(job_id)
        if not raw:
            return None
        data = json.loads(raw)
        zb = data.get("zip_bytes")
        if zb is not None:
            data["zip_bytes"] = io.BytesIO(base64.b64decode(zb))
        return data

