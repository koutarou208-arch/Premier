from __future__ import annotations

import argparse
import json
import mimetypes
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .engine import PACKAGE_ROOT, ScoutEngine


WEB_ROOT = PACKAGE_ROOT / "web"


class ScoutHandler(BaseHTTPRequestHandler):
    engine = ScoutEngine()

    def log_message(self, format: str, *args: Any) -> None:
        # Access logs stay on stderr and never corrupt MCP/API JSON payloads.
        super().log_message(format, *args)

    def _json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _file(self, path: Path) -> None:
        if not path.is_file() or WEB_ROOT.resolve() not in path.resolve().parents:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        body = path.read_bytes()
        mime, _ = mimetypes.guess_type(path.name)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", f"{mime or 'application/octet-stream'}; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        if parsed.path == "/api/health":
            return self._json(self.engine.health())
        if parsed.path == "/api/report":
            query = params.get("q", [""])[0]
            budget = float(params.get("budget_m", ["80"])[0])
            return self._json(self.engine.analyze(query=query, budget_m=budget))
        if parsed.path == "/api/search":
            query = params.get("q", [""])[0]
            if not query:
                return self._json({"error": "q is required"}, HTTPStatus.BAD_REQUEST)
            kinds = set(params.get("kind", [])) or None
            top_k = min(25, max(1, int(params.get("top_k", ["8"])[0])))
            return self._json({"query": query, "results": self.engine.search(query, top_k=top_k, kinds=kinds)})
        if parsed.path == "/api/graph":
            return self._json(self.engine.graph())
        if parsed.path == "/api/workflow":
            return self._json(self.engine.workflow_graph())
        if parsed.path == "/api/semantic-layer":
            return self._json(self.engine.semantic_catalog())
        if parsed.path == "/api/observability":
            return self._json(self.engine.traces.snapshot())
        relative = "index.html" if parsed.path in {"", "/"} else parsed.path.lstrip("/")
        return self._file(WEB_ROOT / relative)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Arsenal Scout API and dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8765, type=int)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), ScoutHandler)
    print(f"Arsenal Scout: http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
