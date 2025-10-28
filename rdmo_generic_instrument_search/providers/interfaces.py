from typing import Protocol, TypedDict


class Option(TypedDict):
    id: str
    text: str


class InstrumentProvider(Protocol):
    id_prefix: str
    text_prefix: str | None
    base_url: str
    max_hits: int

    # RDMO optionset-facing
    def search(self, query: str) -> list[Option]: ...
    # Handler-facing detail
    def detail(self, remote_id: str) -> dict: ...
