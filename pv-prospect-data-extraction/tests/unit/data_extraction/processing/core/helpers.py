"""Shared test helpers."""

import io
from decimal import Decimal
from typing import Iterable

from pv_prospect.common.domain.location import Location
from pv_prospect.common.domain.pv_site import (
    PanelGeometry,
    PVSite,
    Shading,
    System,
)


class FakeTimeSeriesDescriptor:
    def __init__(self, name: str) -> None:
        self._name = name

    def __str__(self) -> str:
        return self._name


def make_pv_site(pvo_sys_id: int = 42248) -> PVSite:
    return PVSite(
        pvo_sys_id=pvo_sys_id,
        name='Test Site',
        location=Location(latitude=Decimal('51.6'), longitude=Decimal('-4.2')),
        shading=Shading.NONE,
        panel_system=System(brand='TestBrand', capacity=4000),
        panel_geometries=[PanelGeometry(azimuth=180, tilt=35, area_fraction=1.0)],
        inverter_system=System(brand='TestInverter', capacity=3600),
    )


class FakeExtractor:
    """Minimal Extractor that returns canned file contents."""

    def __init__(self, files: dict[str, str] | None = None) -> None:
        self._files = files or {}

    def read_file(self, file_path: str) -> io.TextIOWrapper:  # type: ignore[override]
        if file_path not in self._files:
            raise FileNotFoundError(file_path)
        return io.StringIO(self._files[file_path])  # type: ignore[return-value]

    def file_exists(self, file_path: str) -> bool:
        return file_path in self._files


class FakeLoader:
    """Minimal Loader that records calls."""

    def __init__(self, existing_files: set[str] | None = None) -> None:
        self.created_folders: list[str] = []
        self.written_texts: dict[str, str] = {}
        self.written_csvs: dict[str, list[list[str]]] = {}
        self._existing = existing_files or set()

    def create_folder(self, folder_path: str) -> str | None:
        self.created_folders.append(folder_path)
        return folder_path

    def file_exists(self, file_path: str) -> bool:
        return file_path in self._existing

    def write_text(self, file_path: str, text: str, overwrite: bool = False) -> None:
        self.written_texts[file_path] = text

    def write_csv(
        self, file_path: str, rows: Iterable[Iterable[str]], overwrite: bool = False
    ) -> None:
        self.written_csvs[file_path] = [list(row) for row in rows]
