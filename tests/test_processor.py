from async_processor import InputMessage, InputMessageV2
from async_processor.processor import BaseProcessor


def test_default_input_deserializer():
    p = BaseProcessor()

    assert p.input_deserializer(
        serialized_input_message='{"request_id": "a", "body": "a"}'
    ) == InputMessage(request_id="a", body="a")

    assert p.input_deserializer(
        serialized_input_message='{"a": "b"}'
    ) == InputMessageV2(a="b")
