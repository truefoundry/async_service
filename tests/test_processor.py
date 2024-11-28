from async_processor import InputMessage
from async_processor.processor import BaseProcessor


def test_default_input_deserializer():
    p = BaseProcessor()

    assert p.input_deserializer(serialized_input_message='{"a": "b"}') == InputMessage(
        a="b"
    )
