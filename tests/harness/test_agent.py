from __future__ import annotations

import concurrent.futures
import json
import socket
import time
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.harness.agent_emulator import AgentEmulator, delay, status

pytestmark = [pytest.mark.agent, pytest.mark.socket]


def client(emulator: AgentEmulator) -> httpx.Client:
    return httpx.Client(
        transport=httpx.HTTPTransport(uds=emulator.socket_path),
        base_url="http://localhost",
        timeout=2,
    )


def outcome(message: dict[str, Any], status: str = "processed", **extra: object) -> dict[str, Any]:
    return {
        "messageId": message["messageId"],
        "receiptHandle": message["receiptHandle"],
        "status": status,
        **extra,
    }


def test_delivery_processed_and_snapshots() -> None:
    with AgentEmulator.start(poll_wait=0.02) as emulator, client(emulator) as http:
        before = time.monotonic()
        assert http.get("/next").status_code == 204
        assert time.monotonic() - before >= 0.015
        mid = emulator.enqueue('{"hello":"world"}', queue_url="https://sqs/test")
        delivered = http.get("/next").json()
        assert delivered == {
            "messageId": mid,
            "receiptHandle": emulator.message(mid).receipt_handle,
            "body": '{"hello":"world"}',
            "queueUrl": "https://sqs/test",
            "attributes": {"ApproximateReceiveCount": "1"},
        }
        assert len(emulator.in_flight()) == 1
        assert not emulator.pending()
        assert http.post("/result", json=outcome(delivered)).status_code == 200
        assert http.get("/next").status_code == 204
        assert not emulator.in_flight()
        assert not emulator.pending()
        assert emulator.wait_for_result(mid, 0.1).response_code == 200
        snapshot = emulator.message(mid)
        assert snapshot.history == ["queued", "delivered", "processed"]
        snapshot.history.clear()
        assert emulator.message(mid).history
        emulator.results.clear()
        assert len(emulator.results) == 1
        assert http.post("/result", json=outcome(delivered)).status_code == 404
        with pytest.raises(TimeoutError):
            emulator.wait_for_result("not-posted", 0.01)


def test_visibility_expiry_and_stale_receipts() -> None:
    with AgentEmulator.start(poll_wait=0.1, visibility_timeout=0.05) as emu, client(emu) as http:
        mid = emu.enqueue("body")
        first = http.get("/next").json()
        second = http.get("/next").json()
        assert second["messageId"] == mid
        assert second["receiptHandle"] != first["receiptHandle"]
        assert second["attributes"] == {"ApproximateReceiveCount": "2"}
        assert http.post("/result", json=outcome(first)).status_code == 404
        assert http.post("/result", json=outcome(second)).status_code == 200
        assert emu.message(mid).history == [
            "queued",
            "delivered",
            "expired",
            "delivered",
            "processed",
        ]


def test_release_delay() -> None:
    with AgentEmulator.start(poll_wait=0.01) as emu, client(emu) as http:
        mid = emu.enqueue("body")
        first = http.get("/next").json()
        before = time.monotonic()
        assert http.post("/result", json=outcome(first, "released", delay=1)).status_code == 200
        assert len(emu.pending()) == 1
        assert http.get("/next").status_code == 204
        emu.poll_wait = 2
        second = http.get("/next").json()
        assert time.monotonic() - before >= 1
        assert second["messageId"] == mid
        assert second["receiptHandle"] != first["receiptHandle"]
        assert http.post("/result", json=outcome(second, "released", delay=0)).status_code == 200
        third = http.get("/next").json()
        assert third["attributes"]["ApproximateReceiveCount"] == "3"


def test_heartbeat_and_enqueue_wakes_long_poll() -> None:
    with (
        AgentEmulator.start(poll_wait=0.05, visibility_timeout=None) as emu,
        concurrent.futures.ThreadPoolExecutor() as pool,
        client(emu) as http,
    ):
        future = pool.submit(http.get, "/next")
        emu.enqueue("body", delay=0.02)
        assert future.result().status_code == 200
        assert http.get("/next").status_code == 204
        assert len(emu.in_flight()) == 1


@pytest.mark.parametrize(
    "patch",
    [
        {"queueUrl": "x"},
        {"failed": True},
        {"status": "failed"},
        {"status": []},
        {"messageId": 1},
        {"receiptHandle": None},
        {"receiptHandle": []},
        {"delay": None},
        {"delay": -1},
        {"delay": True},
        {"delay": 1.1},
        {"delay": "0"},
    ],
)
def test_result_strict_validation(patch: dict[str, object]) -> None:
    with AgentEmulator.start() as emu, client(emu) as http:
        emu.enqueue("body")
        message = http.get("/next").json()
        assert http.post("/result", json=outcome(message) | patch).status_code == 422
        assert len(emu.in_flight()) == 1
        assert emu.results[0].response_code == 422


@pytest.mark.parametrize("body", [b"{", b"[]", b'"x"', b"null", b"{}", b'{"status":"processed"}'])
def test_invalid_result_bodies_are_recorded(body: bytes) -> None:
    with AgentEmulator.start() as emu, client(emu) as http:
        assert http.post("/result", content=body).status_code == 422
        assert emu.results[0].raw_body == body


def test_unknown_and_stale_codes_configurable() -> None:
    with (
        AgentEmulator.start(unknown_message_status=410, stale_receipt_status=409) as emu,
        client(emu) as http,
    ):
        assert (
            http.post("/result", json={"messageId": "absent", "status": "processed"}).status_code
            == 410
        )
        mid = emu.enqueue("body")
        assert (
            http.post("/result", json={"messageId": mid, "status": "processed"}).status_code == 409
        )


@pytest.mark.parametrize(
    "kind",
    [
        "malformed_json",
        "non_object_json",
        "missing_message_id",
        "empty_message_id",
        "non_string_fields",
    ],
)
def test_fault_responses(kind: str) -> None:
    with AgentEmulator.start(poll_wait=0) as emu, client(emu) as http:
        mid = emu.enqueue("unaffected")
        emu.inject("next", kind)
        response = http.get("/next")
        assert response.status_code == 200
        if kind == "malformed_json":
            with pytest.raises(json.JSONDecodeError):
                response.json()
        elif kind == "non_object_json":
            assert response.json() == "x"
        elif kind == "missing_message_id":
            assert "messageId" not in response.json()
        elif kind == "empty_message_id":
            assert response.json()["messageId"] == ""
        else:
            assert not isinstance(response.json()["receiptHandle"], str)
            assert not isinstance(response.json()["body"], str)
        assert emu.faults_fired[0][1].kind == kind
        assert http.get("/next").json()["messageId"] == mid


def test_fault_order_count_delay_and_disconnect() -> None:
    with AgentEmulator.start(poll_wait=0) as emu, client(emu) as http:
        emu.inject("next", "disconnect", times=2)
        emu.inject("next", status(503, "unhealthy"))
        emu.inject("next", delay(0.03))
        for _ in range(2):
            with pytest.raises(httpx.RemoteProtocolError):
                http.get("/next")
        response = http.get("/next")
        assert response.status_code == 503
        assert response.text == "unhealthy"
        before = time.monotonic()
        assert http.get("/next").status_code == 204
        assert time.monotonic() - before >= 0.025
        assert [f.kind for _, f in emu.faults_fired] == [
            "disconnect",
            "disconnect",
            "status",
            "delay",
        ]


def test_result_disconnect_and_lost_response() -> None:
    with AgentEmulator.start() as emu, client(emu) as http:
        mid = emu.enqueue("body")
        request = outcome(http.get("/next").json())
        emu.inject("result", "disconnect")
        with pytest.raises(httpx.RemoteProtocolError):
            http.post("/result", json=request)
        assert emu.message(mid).status == "in_flight"
        emu.inject("result", status(500))
        assert http.post("/result", json=request).status_code == 500
        emu.inject("result", "apply_then_disconnect")
        with pytest.raises(httpx.RemoteProtocolError):
            http.post("/result", json=request)
        assert emu.message(mid).status == "processed"
        assert emu.results[-1].applied_code == 200
        assert emu.results[-1].response_code is None
        assert http.post("/result", json=request).status_code == 404
        assert len(emu.results) == 4


def test_wait_for_result_waits_for_application() -> None:
    with (
        AgentEmulator.start() as emu,
        client(emu) as http,
        concurrent.futures.ThreadPoolExecutor() as pool,
    ):
        mid = emu.enqueue("body")
        request = outcome(http.get("/next").json())
        emu.inject("result", delay(0.03))
        future = pool.submit(http.post, "/result", json=request)
        result = emu.wait_for_result(mid)
        assert result.completed
        assert result.response_code == 200
        assert emu.message(mid).status == "processed"
        assert future.result().status_code == 200


def test_hang_ends_on_stop_and_idle_connections_close() -> None:
    with AgentEmulator.start() as emu, client(emu) as http:
        path = Path(emu.socket_path)
        emu.inject("next", "hang")
        with pytest.raises(httpx.ReadTimeout):
            http.get("/next", timeout=0.03)
        assert emu.faults_fired[0][1].kind == "hang"
        before = time.monotonic()
        emu.stop()
        assert time.monotonic() - before < 1
        assert not path.exists()
    assert not path.parent.exists()


def test_concurrent_delivery_is_exclusive() -> None:
    with AgentEmulator.start(poll_wait=0.01, visibility_timeout=None) as emu:
        mid = emu.enqueue("body")

        def receive() -> httpx.Response:
            with client(emu) as http:
                return http.get("/next")

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(lambda _: receive(), range(8)))
        assert sorted(r.status_code for r in responses) == [200] + [204] * 7
        assert emu.message(mid).receive_count == 1


def test_preexisting_socket_and_replacement_are_never_removed(tmp_path: Path) -> None:
    # Use the short private directory allocated by the harness (macOS UDS path limit).
    with AgentEmulator.start() as emu:
        path = Path(emu.socket_path)
        with pytest.raises(OSError):
            AgentEmulator.start(path)
        assert path.exists()
        path.unlink()
        path.write_text("replacement")
        emu.stop()
        assert path.read_text() == "replacement"
        path.unlink()
    regular = tmp_path / "keep"
    regular.write_text("existing")
    with pytest.raises(OSError):
        AgentEmulator.start(regular)
    assert regular.read_text() == "existing"


def test_stop_closes_incomplete_http_request() -> None:
    with AgentEmulator.start() as emu, socket.socket(socket.AF_UNIX) as stream:
        stream.connect(emu.socket_path)
        stream.sendall(b"POST /result HTTP/1.1\r\nContent-Length: 100\r\n\r\n{")
        emu.stop()
        assert not Path(emu.socket_path).exists()


@pytest.mark.parametrize("redeliver", [False, True])
def test_omitted_receipt_rejected_for_current_delivery(redeliver):
    with AgentEmulator.start(stale_receipt_status=409) as emu, client(emu) as http:
        mid = emu.enqueue("body")
        current = http.get("/next").json()
        if redeliver:
            assert http.post("/result", json=outcome(current, "released")).status_code == 200
            current = http.get("/next").json()
        assert (
            http.post("/result", json={"messageId": mid, "status": "processed"}).status_code == 409
        )
        assert len(emu.in_flight()) == 1
        assert http.post("/result", json=outcome(current)).status_code == 200


@pytest.mark.parametrize(
    ("status", "delay", "expected"),
    [("processed", 0, 422), ("released", 43201, 422), ("released", 43200, 200)],
)
def test_result_delay_contract(status, delay, expected):
    with AgentEmulator.start() as emu, client(emu) as http:
        emu.enqueue("body")
        message = http.get("/next").json()
        assert (
            http.post("/result", json=outcome(message, status, delay=delay)).status_code == expected
        )
        assert bool(emu.in_flight()) == (expected == 422)
