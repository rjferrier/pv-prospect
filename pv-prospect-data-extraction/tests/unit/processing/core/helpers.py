"""Shared test helpers."""

import csv
import io
from decimal import Decimal

from pv_prospect.common.domain import Location, PanelGeometry, PVSite, Shading, System
from pv_prospect.etl.storage import FileEntry


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


class FakeFileSystem:
    """Minimal FileSystem that returns canned file contents and records writes."""

    def __init__(self, files: dict[str, str] | None = None) -> None:
        self._files = files or {}
        self.created_folders: list[str] = []
        self.written_texts: dict[str, str] = {}

    def exists(self, path: str) -> bool:
        return path in self._files

    def read_text(self, path: str) -> str:
        if path not in self._files:
            raise FileNotFoundError(path)
        return self._files[path]

    def write_text(self, path: str, content: str) -> None:
        self.written_texts[path] = content

    def mkdir(self, path: str) -> None:
        self.created_folders.append(path)

    def list_files(
        self, prefix: str, pattern: str = '*', recursive: bool = False
    ) -> list[FileEntry]:
        return []

    @property
    def written_csv_rows(self) -> dict[str, list[list[str]]]:
        return {
            path: list(csv.reader(io.StringIO(text)))
            for path, text in self.written_texts.items()
        }
