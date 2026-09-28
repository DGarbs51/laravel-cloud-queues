from __future__ import annotations

import json

import pytest

from laravel_cloud_queues.errors import (
    DispatchError,
    JobDefectError,
    MalformedEnvelopeError,
    SerializationError,
    UnsupportedEnvelopeVersionError,
    UnsupportedOverflowPayloadError,
)
from laravel_cloud_queues.jobs import envelope as module
from laravel_cloud_queues.jobs.envelope import (
    MAX_DEPTH,
    Envelope,
    decode_envelope,
    encode_envelope,
    peek_display_name,
)
from laravel_cloud_queues.jobs.policy import RetryPolicy


@pytest.fixture(autouse=True)
def policy_stub(monkeypatch):
    # L3c owns policy.py; replace only its unimplemented post-init in this checkout.
    try:
        RetryPolicy()
    except NotImplementedError:
        monkeypatch.setattr(RetryPolicy, "__post_init__", lambda self: None)


def wire():
    return {
        "uuid": "logical-job-id",
        "displayName": "jobs.send",
        "laravel_cloud_queues": {
            "version": 1,
            "job": "jobs.send",
            "args": [],
            "kwargs": {},
        },
    }


def test_dashboard_retry_and_policy_survive_roundtrip():
    """D3/D4: failed payload remains intact; attempts belong to the delivery.

    Laravel src/Illuminate/Foundation/Cloud/FailedJobProvider.php:81-85 emits the
    original payload and reads its top-level displayName (pinned v13.33.0).
    """
    original = Envelope(
        uuid="one",
        display_name="café",
        job="café",
        args=(1, "hello"),
        kwargs={"value": [2]},
        policy=RetryPolicy(tries=3, backoff=(1, 5), timeout=60, fail_on_timeout=False),
        queue="emails",
        dispatched_at="2026-09-27T12:00:00+00:00",
        context={"traceparent": "trace"},
    )
    body = encode_envelope(original)
    restored = decode_envelope(body)
    assert encode_envelope(restored) == body
    assert restored.policy.tries == 3
    assert restored.policy.backoff == (1, 5)
    assert restored.policy.timeout == 60
    assert restored.policy.fail_on_timeout is False
    assert restored.args == original.args
    assert restored.context == original.context
    assert "café" in body
    assert ": " not in body
    assert json.loads(body)["uuid"] == "one"
    assert json.loads(body)["displayName"] == "café"


def test_policy_omission_and_zero_values():
    body = encode_envelope(Envelope(uuid="u", display_name="j", job="j"))
    section = json.loads(body)["laravel_cloud_queues"]
    assert section["policy"] == {}
    assert "queue" not in section
    assert "dispatched_at" not in section
    assert decode_envelope(body).policy == RetryPolicy()
    value = wire()
    value["laravel_cloud_queues"]["policy"] = {
        "tries": 0,
        "timeout": 0,
        "backoff": 0,
        "fail_on_timeout": True,
        "retry_until": {"future": "ignored"},
        "max_exceptions": "reserved",
    }
    decoded = decode_envelope(json.dumps(value))
    assert decoded.policy == RetryPolicy(tries=0, timeout=0, backoff=0, fail_on_timeout=True)
    assert decoded.extra["policy"]["max_exceptions"] == "reserved"


def test_namespaced_unknown_keys_do_not_collide():
    value = wire()
    value["future"] = "top"
    value["attempts"] = 99
    value["laravel_cloud_queues"]["future"] = "section"
    value["laravel_cloud_queues"]["attempts"] = 100
    decoded = decode_envelope(json.dumps(value))
    assert decoded.extra["top_level"]["future"] == "top"
    assert decoded.extra["section"]["future"] == "section"
    assert json.loads(encode_envelope(decoded))["attempts"] == 99
    assert encode_envelope(decode_envelope(encode_envelope(decoded))) == encode_envelope(decoded)


@pytest.mark.parametrize(
    "body",
    [
        "",
        "null",
        "[]",
        "true",
        "1",
        '"text"',
        "{}",
        '{"laravel_cloud_queues":[]}',
        '{"uuid":"one","uuid":"two"}',
        '{"x":{"a":1,"a":2}}',
        '{"x":NaN}',
        '{"x":Infinity}',
        '{"x":-Infinity}',
        '{"x":1e999}',
        '{"x":"bad\x00"}',
        '{"x":01}',
        '{"x":1} trailing',
        "\ud800",
    ],
)
def test_malformed_json_is_a_job_defect(body):
    with pytest.raises(MalformedEnvelopeError) as exc:
        decode_envelope(body)
    assert isinstance(exc.value, JobDefectError)
    assert not isinstance(exc.value, DispatchError)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("version", True),
        ("version", "1"),
        ("version", 1.0),
        ("version", None),
        ("job", 2),
        ("args", {}),
        ("kwargs", []),
        ("policy", []),
        ("context", []),
        ("context", {"x": 1}),
        ("queue", None),
        ("dispatched_at", 4),
    ],
)
def test_section_shape(key, value):
    data = wire()
    data["laravel_cloud_queues"][key] = value
    with pytest.raises(MalformedEnvelopeError):
        decode_envelope(json.dumps(data))


@pytest.mark.parametrize("key", ["uuid", "displayName"])
@pytest.mark.parametrize("value", [None, 3, [], {}])
def test_top_level_strings(key, value):
    data = wire()
    data[key] = value
    with pytest.raises(MalformedEnvelopeError):
        decode_envelope(json.dumps(data))


@pytest.mark.parametrize("key", ["uuid", "displayName", "version", "job", "args", "kwargs"])
def test_required_fields(key):
    data = wire()
    del (data if key in data else data["laravel_cloud_queues"])[key]
    with pytest.raises(MalformedEnvelopeError):
        decode_envelope(json.dumps(data))


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("tries", True),
        ("tries", 1.0),
        ("tries", -1),
        ("tries", None),
        ("backoff", False),
        ("backoff", -0.1),
        ("backoff", [1, -1]),
        ("backoff", [True]),
        ("backoff", "1"),
        ("backoff", None),
        ("timeout", True),
        ("timeout", -1),
        ("timeout", "60"),
        ("timeout", None),
        ("fail_on_timeout", 1),
        ("fail_on_timeout", None),
    ],
)
def test_invalid_policy(key, value):
    data = wire()
    data["laravel_cloud_queues"]["policy"] = {key: value}
    with pytest.raises(MalformedEnvelopeError):
        decode_envelope(json.dumps(data))


def test_unsupported_version():
    data = wire()
    data["laravel_cloud_queues"]["version"] = 2
    with pytest.raises(UnsupportedEnvelopeVersionError) as exc:
        decode_envelope(json.dumps(data))
    assert exc.value.version == 2


@pytest.mark.parametrize("value", ["laravel:sqs-payloads:uuid", None, {}, 3])
def test_pointer_takes_precedence_over_missing_envelope(value):
    """Laravel src/Illuminate/Queue/SqsQueue.php:567 emits an @pointer object.

    v1 deliberately rejects all pointer bodies; no cache/network hydration.
    """
    with pytest.raises(UnsupportedOverflowPayloadError):
        decode_envelope(json.dumps({"@pointer": value}))


def test_million_deep_body_rejected_before_parser(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Deep body reached recursive JSON parser")

    monkeypatch.setattr(module.json, "loads", forbidden)
    with pytest.raises(MalformedEnvelopeError):
        decode_envelope("[" * 1_000_000 + "]" * 1_000_000)


def test_depth_boundary_and_string_brackets():
    data = wire()
    data["extra"] = "[" * 100 + '\\"' + "]" * 100
    body = json.dumps(data)
    assert decode_envelope(body).uuid == "logical-job-id"
    for count, valid in ((MAX_DEPTH - 1, True), (MAX_DEPTH, False)):
        body = '{"extra":' + "[" * count + "0" + "]" * count + "," + json.dumps(wire())[1:]
        if valid:
            assert decode_envelope(body).uuid == "logical-job-id"
        else:
            with pytest.raises(MalformedEnvelopeError):
                decode_envelope(body)


def test_size_is_utf8_bytes_and_precedes_parse(monkeypatch):
    body = json.dumps(wire(), ensure_ascii=False).replace("jobs.send", "café")
    monkeypatch.setattr(module, "MAX_BODY_BYTES", len(body.encode("utf-8")))
    assert decode_envelope(body).display_name == "café"
    monkeypatch.setattr(module, "MAX_BODY_BYTES", len(body.encode("utf-8")) - 1)
    with pytest.raises(MalformedEnvelopeError):
        decode_envelope(body)
    monkeypatch.setattr(module.json, "loads", lambda *a, **kw: pytest.fail("Oversize parsed"))
    with pytest.raises(MalformedEnvelopeError):
        decode_envelope(body)


def test_peek_is_safe_and_independent_of_envelope_version():
    """Laravel src/Illuminate/Foundation/Cloud/FailedJobProvider.php:85 reads displayName."""
    assert peek_display_name('{"displayName":"send","version":999}') == "send"
    for body in (
        '{"displayName":3}',
        '{"displayName":"a","displayName":"b"}',
        "[" * 1_000_000,
        "garbage",
    ):
        assert peek_display_name(body) == ""


def test_encode_rejects_nonfinite_json():
    with pytest.raises(SerializationError):
        encode_envelope(Envelope(uuid="u", display_name="j", job="j", args=(float("nan"),)))


def test_policy_constructor_failure_is_a_job_defect(monkeypatch):
    from laravel_cloud_queues.errors import ConfigurationError

    def reject(self):
        raise ConfigurationError("Invalid declared policy")

    monkeypatch.setattr(RetryPolicy, "__post_init__", reject)
    with pytest.raises(MalformedEnvelopeError):
        decode_envelope(json.dumps(wire()))


@pytest.mark.parametrize("timeout", [604800.1, 604801, 2**31, 1e308])
def test_timeout_above_seven_days_is_malformed(timeout):
    data = wire()
    data["laravel_cloud_queues"]["policy"] = {"timeout": timeout}
    with pytest.raises(MalformedEnvelopeError):
        decode_envelope(json.dumps(data))


@pytest.mark.parametrize("timeout", [0, 604800])
def test_timeout_boundaries_are_valid(timeout):
    data = wire()
    data["laravel_cloud_queues"]["policy"] = {"timeout": timeout}
    assert decode_envelope(json.dumps(data)).policy.timeout == timeout


@pytest.mark.parametrize("surrogate", [chr(0xD800), chr(0xDFFF)])
def test_encode_rejects_lone_surrogates(surrogate):
    with pytest.raises(SerializationError):
        encode_envelope(Envelope(uuid="u", display_name="j", job="j", args=(surrogate,)))


def test_any_policy_constructor_exception_is_a_job_defect(monkeypatch):
    def reject(self):
        raise RuntimeError("Policy construction failed")

    monkeypatch.setattr(RetryPolicy, "__post_init__", reject)
    with pytest.raises(MalformedEnvelopeError):
        decode_envelope(json.dumps(wire()))
