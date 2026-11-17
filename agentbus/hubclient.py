"""Small stdlib client for the hub HTTP API (SPEC.md 'Hub HTTP API').

Transport failures raise HubError with status 0. HTTP errors raise HubError with
the HTTP status and the hub's error code (body {"error": "..."} when present).
"""

import http.client
import json
import socket
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_TIMEOUT = 10.0


class HubError(Exception):
    def __init__(self, status, code, message=""):
        self.status = status
        self.code = code
        self.message = message
        super().__init__("hub %s %s%s" % (status, code, (": " + message) if message else ""))


def _parse_error(raw, fallback):
    try:
        body = json.loads(raw.decode("utf-8") or "{}")
    except ValueError:
        body = None
    if isinstance(body, dict):
        code = body.get("error") or fallback
        msg = body.get("message") or ""
        return str(code), str(msg)
    return str(fallback or "http_error"), raw[:200].decode("utf-8", "replace")


class HubClient:
    def __init__(self, base_url, token="", timeout=DEFAULT_TIMEOUT):
        self.base_url = base_url.rstrip("/")
        self.token = token or ""
        self.timeout = timeout
        # Bypass env proxies: the hub is reached on 127.0.0.1 through a tunnel.
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def _call(self, method, path, body=None, query=None, timeout=None, auth=True):
        url = self.base_url + path
        if query:
            url += "?" + urllib.parse.urlencode(query)
        headers = {"Accept": "application/json"}
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if auth and self.token:
            headers["Authorization"] = "Bearer " + self.token
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with self._opener.open(req, timeout=timeout or self.timeout) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            try:
                raw = e.read()
            finally:
                e.close()
            code, msg = _parse_error(raw, e.reason)
            raise HubError(e.code, code, msg)
        except (urllib.error.URLError, http.client.HTTPException, OSError) as e:
            raise HubError(0, "unreachable", str(e))
        try:
            return json.loads(raw.decode("utf-8") or "{}")
        except ValueError as e:
            raise HubError(0, "bad_response", str(e))

    def health(self):
        return self._call("GET", "/v1/health", auth=False)

    def register(self, body):
        return self._call("POST", "/v1/register", body=body)

    def heartbeat(self, machine, live):
        return self._call("POST", "/v1/heartbeat", body={"machine": machine, "live": list(live)})

    def peers(self):
        return self._call("GET", "/v1/peers")

    def send(self, from_key, to, text, client_msg_id):
        return self._call(
            "POST",
            "/v1/send",
            body={"from_key": from_key, "to": to, "text": text, "client_msg_id": client_msg_id},
        )

    def pull(self, machine, wait):
        # Long poll: the hub holds the request up to `wait` seconds.
        return self._call(
            "GET",
            "/v1/pull",
            query={"machine": machine, "wait": int(wait)},
            timeout=wait + 10,
        )

    def report(self, msg_id, state, detail=None):
        body = {"msg_id": msg_id, "state": state}
        if detail is not None:
            body["detail"] = detail
        return self._call("POST", "/v1/report", body=body)

    def status(self, msg_id):
        return self._call("GET", "/v1/status", query={"msg_id": msg_id})

    # Reliability protocol 2 never silently downgrades to v1.
    def control_submit2(self, body):
        return self._call('POST', '/v2/control/submit', body=body)

    def control_pull2(self, machine, owner):
        return self._call('GET', '/v2/control/pull', query={'machine':machine,'owner':owner})

    def control_begin2(self, body):
        return self._call('POST', '/v2/control/begin', body=body)

    def control_report2(self, body):
        return self._call('POST', '/v2/control/report', body=body)

    def control_status2(self, control_id, session_key):
        return self._call('GET', '/v2/control/status', query={'control_id':control_id,'session_key':session_key})

    def send2(self, body):
        return self._call('POST', '/v2/send', body=body)

    def pull2(self, machine, wait=1):
        return self._call('GET', '/v2/pull', query={'machine': machine, 'wait': wait}, timeout=wait + 10)

    def report2(self, body):
        return self._call('POST', '/v2/report', body=body)

    def receive2(self, body):
        return self._call('POST', '/v2/receive', body=body)

    def ack2(self, body):
        return self._call('POST', '/v2/ack', body=body)

    def status2(self, msg_id, session_key):
        return self._call('GET', '/v2/status', query={'msg_id': msg_id, 'session_key': session_key})

    def message2(self, msg_id, session_key):
        return self._call('GET', '/v2/message', query={'msg_id': msg_id, 'session_key': session_key})

    def inbox2(self, session_key, after=0, limit=50):
        return self._call('GET', '/v2/inbox', query={'session_key': session_key, 'after': after, 'limit': limit})

    def history2(self, msg_id, session_key, after=0, limit=50):
        return self._call('GET', '/v2/history', query={'msg_id': msg_id, 'session_key': session_key, 'after': after, 'limit': limit})
