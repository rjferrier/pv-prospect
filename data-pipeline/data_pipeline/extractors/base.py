from dataclasses import dataclass


@dataclass(frozen=True)
class ExtractionResult:
    data: list[list[str]]
    metadata: dict | None = None
