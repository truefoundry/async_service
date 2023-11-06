from async_processor import InputMessage, InputMessageV2


def test_input_message_get_body():
    assert InputMessage(request_id="a", body="b").get_body() == "b"

    assert InputMessageV2(tfy_request_id="a", foo="bar", baz=[1, 2]).get_body() == {
        "foo": "bar",
        "baz": [1, 2],
    }
