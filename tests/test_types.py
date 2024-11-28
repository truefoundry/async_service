from async_processor import InputMessage


def test_input_message_get_body():
    assert InputMessage(tfy_request_id="a", foo="bar", baz=[1, 2]).get_body() == {
        "foo": "bar",
        "baz": [1, 2],
    }
