from typing import List

from pydantic import BaseModel, Field
from typing_extensions import Annotated

from async_processor.function_service.utils import (
    create_pydantic_model_from_function_signature,
)

# def dummy_function(a: Annotated[int, ], b: str, c: bool = True, d: List[str] = []) -> None:
#    pass


def dummy_function_with_annotations(
    a: Annotated[int, Field(description="a description")],
    b: str,
    d: List[str],
    c: bool = True,
) -> None:
    pass


class DummyModel(BaseModel):
    a: int = Field(description="a description")
    b: str
    c: bool = True
    d: List[str]


def test_create_pydantic_model_from_function_signature():
    pydantic_model = create_pydantic_model_from_function_signature(
        model_name="DummyModel", func=dummy_function_with_annotations
    )

    assert pydantic_model.schema_json() == DummyModel.schema_json()
