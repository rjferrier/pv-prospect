import csv
import io
import json
import math
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from urllib.request import urlopen

import geopandas as gpd
import numpy as np
from shapely.geometry import Point
from shapely.prepared import prep

DEFAULT_LON_RESOLUTION = 0.04
DEFAULT_LAT_RESOLUTION = 0.04

WRITE_SAMPLES = True
WRITE_CELLS = False

PLOT_SAMPLES = False
PLOT_ALL_POINTS = False
PLOT_PACKED_CELLS = False
PLOT_SAMPLE_DETAILS = True

REPORT_PACKED_CELLS = False
COLOR_PACKED_CELLS = False

MARKER_STYLE = '.'
MARKER_SIZE = 2
LEFT_LIMIT = -10
TOP_LIMIT = 59.5
PLOT_DPI = 600
PLOT_FORMAT = 'png'
PNG_PALETTE_COMPRESS = True

NATURAL_EARTH_10M_URL = (
    'https://naturalearth.s3.amazonaws.com/10m_cultural/ne_10m_admin_0_countries.zip'
)

TEMP_DIR_PREFIX = 'tmp/'
OUTPUT_FILE_FORMAT = 'csv'
CACHE_DIR = Path.home() / '.cache' / 'pv-prospect-geo'


@dataclass(frozen=True)
class GridConfig:
    lat_res: float
    lon_res: float
    origin_lat: float = 51.5
    origin_lon: float = 0.0


@dataclass
class Cell:
    keys: list[tuple[int, int]]
    points: list[tuple[float, float]]
    packed: bool = False


@dataclass(frozen=True)
class Box:
    lat_min: float
    lon_min: float
    lat_max: float
    lon_max: float


@dataclass(frozen=True)
class GridControl:
    box: Box
    relative_size: float


NOMINAL_CELL_WIDTH = 4
GRID_CONTROLS = (
    GridControl(
        box=Box(lat_min=50, lon_min=-10, lat_max=54.5, lon_max=2),
        relative_size=1.0
    ),
)
MIN_RELATIVE_SIZE = 0.5
MAX_RELATIVE_SIZE = 2.0

MAX_MERGED_CELLS = 9

DETAIL_BOX = Box(lat_min=49.8, lon_min=-6, lat_max=50.5, lon_max=-4.5)
SAMPLE_DETAIL_RANGE = range(0, 3)
DETAIL_DPI = 150

MAIN_PLOT_DIR = 'plots'
SAMPLES_OUTPUT_DIR = 'point_samples'

CELLS_OUTPUT_DIR = TEMP_DIR_PREFIX + 'grid_cells'
PACKED_CELLS_PLOT_DIR = TEMP_DIR_PREFIX + 'packed_cells_plotted'
SAMPLES_PLOT_DIR = TEMP_DIR_PREFIX + 'point_samples_plotted'

# Bounding boxes for the UK (rough, used to limit the search space).
# Each is (min_lat, max_lat, min_lon, max_lon).
UK_BOUNDING_BOXES = [
    # Great Britain mainland + nearby islands
    (49.9, 58.7, -8.2, 1.8),
    # Northern Ireland
    (54.0, 55.4, -8.2, -5.4),
    # # Shetland
    # (59.8, 60.9, -1.8, -0.7),
    # # Orkney
    # (58.7, 59.4, -3.5, -2.3),
    # # Outer Hebrides
    # (56.8, 58.3, -7.7, -6.1),
]


def _download_shapefile() -> Path:
    cache_path = CACHE_DIR / 'ne_10m_admin_0_countries'
    shp_file = cache_path / 'ne_10m_admin_0_countries.shp'
    if shp_file.exists():
        return shp_file

    print('Downloading Natural Earth 10m countries shapefile...')
    cache_path.mkdir(parents=True, exist_ok=True)
    with urlopen(NATURAL_EARTH_10M_URL) as response:
        data = response.read()
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        zf.extractall(cache_path)
    print('Download complete.')
    return shp_file


def _load_uk_geometry() -> gpd.GeoDataFrame:
    shp_path = _download_shapefile()
    world = gpd.read_file(shp_path)
    return world[world['ADMIN'] == 'United Kingdom']


def _build_aligned_range(
    origin: float, resolution: float, lower: float, upper: float
) -> np.ndarray:
    start_index = math.ceil((lower - origin) / resolution)
    end_index = math.floor((upper - origin) / resolution)
    return origin + np.arange(start_index, end_index + 1) * resolution


def generate_uk_grid(config: GridConfig) -> list[tuple[float, float]]:
    uk_gdf = _load_uk_geometry()
    uk_geometry = uk_gdf.union_all()
    prepared_geometry = prep(uk_geometry)

    seen: set[tuple[float, float]] = set()
    points: list[tuple[float, float]] = []

    for min_lat, max_lat, min_lon, max_lon in UK_BOUNDING_BOXES:
        lats = _build_aligned_range(config.origin_lat, config.lat_res, min_lat, max_lat)
        lons = _build_aligned_range(config.origin_lon, config.lon_res, min_lon, max_lon)

        for lat in lats:
            for lon in lons:
                coord = (round(lat, 6), round(lon, 6))
                if coord in seen:
                    continue
                seen.add(coord)

                if prepared_geometry.contains(Point(coord[1], coord[0])):
                    points.append(coord)

    points.sort()
    return points


CELL_COLOR_POOL = [
    '#e41a1c',
    '#377eb8',
    '#4daf4a',
    '#984ea3',
    '#ff7f00',
    '#a65628',
    '#f781bf',
    '#999999',
]

PACKED_CELL_COLORS = [
    '#ffff33',
    '#00ffff',
    '#ff00ff',
    '#00ff7f',
    '#ff6600',
    '#6600ff',
    '#ff0066',
    '#00ccff',
]


def _save_figure(fig: 'plt.Figure', out_path: Path) -> None:  # type: ignore[name-defined]
    if PLOT_FORMAT == 'png' and PNG_PALETTE_COMPRESS:
        from PIL import Image

        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=PLOT_DPI, bbox_inches='tight')
        buf.seek(0)
        im = Image.open(buf)
        im.convert('RGB').convert('P', palette=Image.ADAPTIVE).save(out_path, format='PNG')
    else:
        fig.savefig(out_path, dpi=PLOT_DPI, bbox_inches='tight')


def plot_cells(
    cells: list[Cell],
    cell_colors: list[int],
    grid_points: list[tuple[float, float]],
    config: GridConfig,
    cell_width: int,
    out_dir: Path,
    palette: list[str] | None = None,
) -> None:
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt

    uk_gdf = _load_uk_geometry()

    fig, ax = plt.subplots(1, 1, figsize=(8, 12))

    num_colors = max(cell_colors) + 1
    if palette is None:
        palette = list(CELL_COLOR_POOL)
        while len(palette) < num_colors:
            palette.append(palette[len(palette) % len(CELL_COLOR_POOL)])

    cell_w = config.lon_res * cell_width
    cell_h = config.lat_res * cell_width

    for cell, color_idx in zip(cells, cell_colors):
        color = palette[color_idx]
        for row, col in cell.keys:
            x = config.origin_lon + col * cell_w
            y = config.origin_lat + row * cell_h
            ax.add_patch(
                mpatches.Rectangle(
                    (x, y),
                    cell_w,
                    cell_h,
                    facecolor=color,
                    edgecolor='none',
                    alpha=0.6,
                )
            )

    uk_gdf.boundary.plot(ax=ax, color='black', linewidth=0.5)

    if PLOT_ALL_POINTS:
        lats = [p[0] for p in grid_points]
        lons = [p[1] for p in grid_points]
        ax.scatter(
            lons,
            lats,
            marker='.',
            s=MARKER_SIZE,
            facecolors='black',
            edgecolors='none',
            # alpha=0.6,
        )

    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_title(
        f'UK Grid Cells — blocks ({len(cells)} cells, {num_colors} colours, '
        f'res={config.lat_res}x{config.lon_res})'
    )
    ax.autoscale_view()
    ax.set_xlim(left=LEFT_LIMIT)
    ax.set_ylim(top=TOP_LIMIT)
    ax.set_aspect('equal')

    col_step = config.lon_res * cell_width
    row_step = config.lat_res * cell_width

    ax.secondary_xaxis(
        'top',
        functions=(
            lambda lon: (lon - config.origin_lon) / col_step,
            lambda col: col * col_step + config.origin_lon,
        ),
    ).set_xlabel('Column index')

    ax.secondary_yaxis(
        'right',
        functions=(
            lambda lat: (lat - config.origin_lat) / row_step,
            lambda row: row * row_step + config.origin_lat,
        ),
    ).set_ylabel('Row index')

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f'all_cells.{PLOT_FORMAT}'

    _save_figure(fig, out_path)
    print(f'Plot saved → {out_path}')
    plt.close(fig)


def plot_samples(
    cells: list[Cell],
    cell_colors: list[int],
    config: GridConfig,
    cell_width: int,
    max_cell_size: int,
) -> None:
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt

    uk_gdf = _load_uk_geometry()

    num_colors = max(cell_colors) + 1
    palette = CELL_COLOR_POOL
    while len(palette) < num_colors:
        palette = palette + [palette[len(palette) % len(CELL_COLOR_POOL)]]

    cell_w = config.lon_res * cell_width
    cell_h = config.lat_res * cell_width

    padded_cells = [_pad_points(c.points, max_cell_size) for c in cells]

    out_dir = Path(SAMPLES_PLOT_DIR)
    if out_dir.exists():
        for f in out_dir.glob('*.png'):
            f.unlink()
    out_dir.mkdir(parents=True, exist_ok=True)

    for i in range(max_cell_size):
        fig, ax = plt.subplots(1, 1, figsize=(8, 12))

        for cell, color_idx in zip(cells, cell_colors):
            color = palette[color_idx]
            for row, col in cell.keys:
                x = config.origin_lon + col * cell_w
                y = config.origin_lat + row * cell_h
                ax.add_patch(
                    mpatches.Rectangle(
                        (x, y),
                        cell_w,
                        cell_h,
                        facecolor=color,
                        edgecolor='none',
                        alpha=0.5,
                    )
                )

        uk_gdf.boundary.plot(ax=ax, color='black', linewidth=0.5)

        for padded, color_idx in zip(padded_cells, cell_colors):
            if i < len(padded):
                lat, lon = padded[i]
                ax.scatter(
                    [lon],
                    [lat],
                    marker=MARKER_STYLE,
                    s=MARKER_SIZE,
                    facecolors='black',
                    edgecolors='none',
                )

        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        ax.set_title(f'Sample {i + 1}/{max_cell_size}')
        ax.autoscale_view()
        ax.set_xlim(left=LEFT_LIMIT)
        ax.set_ylim(top=TOP_LIMIT)
        ax.set_aspect('equal')

        out_path = out_dir / f'sample_{i:03d}.{PLOT_FORMAT}'
        _save_figure(fig, out_path)
        plt.close(fig)

    print(f'Saved {max_cell_size} sample plots → {out_dir}/')


def plot_sample_details(
    cells: list[Cell],
    cell_colors: list[int],
    palette: list[str],
    config: GridConfig,
    cell_width: int,
    max_cell_size: int,
    out_dir: Path,
    sample_range: range = SAMPLE_DETAIL_RANGE,
    box: Box = DETAIL_BOX,
) -> None:
    """Plot a row of subplots, one per iteration in sample_range.

    Each subplot shows the single point sampled from every cell at that
    iteration, clipped to the given box.
    Cell rectangles whose keys overlap the box are shaded for context.
    """
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt

    lat_min, lon_min, lat_max, lon_max = box.lat_min, box.lon_min, box.lat_max, box.lon_max
    uk_gdf = _load_uk_geometry()

    cell_w = config.lon_res * cell_width
    cell_h = config.lat_res * cell_width

    padded_cells = [_pad_points(cell.points, max_cell_size) for cell in cells]

    n = len(sample_range)
    fig, axes = plt.subplots(1, n, figsize=(4 * n, 4))
    if n == 1:
        axes = [axes]

    for ax, i in zip(axes, sample_range):
        for cell, color_idx in zip(cells, cell_colors):
            color = palette[color_idx]
            for row, col in cell.keys:
                x = config.origin_lon + col * cell_w
                y = config.origin_lat + row * cell_h
                if x + cell_w < lon_min or x > lon_max:
                    continue
                if y + cell_h < lat_min or y > lat_max:
                    continue
                ax.add_patch(
                    mpatches.Rectangle(
                        (x, y), cell_w, cell_h,
                        facecolor=color, edgecolor='none', alpha=0.4,
                    )
                )

        uk_gdf.boundary.plot(ax=ax, color='black', linewidth=0.5)

        for padded, color_idx in zip(padded_cells, cell_colors):
            if i < len(padded):
                lat, lon = padded[i]
                if lon_min <= lon <= lon_max and lat_min <= lat <= lat_max:
                    ax.scatter(
                        [lon], [lat],
                        marker=MARKER_STYLE, s=MARKER_SIZE * 10,
                        facecolors=palette[color_idx], edgecolors='black', linewidths=0.4,
                    )

        ax.set_xlim(lon_min, lon_max)
        ax.set_ylim(lat_min, lat_max)
        ax.set_aspect('equal')
        ax.axis('off')
        ax.set_title(f'Sample {i:03d}', fontsize=8)

    fig.subplots_adjust(wspace=0.02)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f'sample_details.{PLOT_FORMAT}'
    fig.savefig(out_path, dpi=DETAIL_DPI, bbox_inches='tight')
    plt.close(fig)
    print(f'Sample detail plot saved → {out_path}')


def write_json(
    config: GridConfig, grid_points: list[tuple[float, float]], out_path: Path
) -> Path:
    output = {
        'lat_res': config.lat_res,
        'lon_res': config.lon_res,
        'origin': {'lat': config.origin_lat, 'lon': config.origin_lon},
        'count': len(grid_points),
        'points': [{'lat': lat, 'lon': lon} for lat, lon in grid_points],
    }
    out_path.write_text(json.dumps(output))


def write_csv(
    _: GridConfig, grid_points: list[tuple[float, float]], out_path: Path
) -> Path:
    with open(out_path, 'w') as f:
        csv.writer(f).writerow(('lat', 'lon'))
        csv.writer(f).writerows(grid_points)


def _assign_coarse_cells(
    points: list[tuple[float, float]],
    config: GridConfig,
    cell_width: int,
) -> dict[tuple[int, int], list[tuple[float, float]]]:
    cells: dict[tuple[int, int], list[tuple[float, float]]] = {}
    for lat, lon in points:
        lat_idx = round((lat - config.origin_lat) / config.lat_res)
        lon_idx = round((lon - config.origin_lon) / config.lon_res)
        cell_row = lat_idx // cell_width
        cell_col = lon_idx // cell_width
        cells.setdefault((cell_row, cell_col), []).append((lat, lon))
    return cells


def _r2_permutation(side: int) -> list[int]:
    """R2 low-discrepancy permutation for a side x side toroidal grid.

    Uses the plastic constant to generate a sequence where each new
    point is far from all previous points, accounting for wrap-around.
    Adjacent cells sharing this permutation will have well-separated
    points at each time step because the sequence is toroidal.
    """
    n = side * side
    rho = 1.32471795724474602596
    alpha1 = 1.0 / rho
    alpha2 = 1.0 / (rho ** 2)

    seen: set[int] = set()
    result: list[int] = []
    i = 1
    while len(result) < n:
        u = (i * alpha1) % 1.0
        v = (i * alpha2) % 1.0
        r = int(u * side)
        c = int(v * side)
        flat = r * side + c
        if flat not in seen:
            seen.add(flat)
            result.append(flat)
        i += 1
    return result


def _scatter_cell_points(
    points: list[tuple[float, float]],
    key: tuple[int, int],
    config: GridConfig,
    cell_width: int,
) -> list[tuple[float, float]]:
    """Reorder points so consecutive entries are spatially far apart.

    Builds the full cell_width x cell_width grid, traverses it in R2
    toroidal sequence order (ghosts hold their slots but are filtered
    from output).
    """
    grid: dict[tuple[int, int], tuple[float, float]] = {}
    cell_row, cell_col = key
    for lat, lon in points:
        lat_idx = round((lat - config.origin_lat) / config.lat_res)
        lon_idx = round((lon - config.origin_lon) / config.lon_res)
        local_r = lat_idx - cell_row * cell_width
        local_c = lon_idx - cell_col * cell_width
        grid[(local_r, local_c)] = (lat, lon)

    perm = _r2_permutation(cell_width)
    return [
        grid[(idx // cell_width, idx % cell_width)]
        for idx in perm
        if (idx // cell_width, idx % cell_width) in grid
    ]


def _interleave(
    a: list[tuple[float, float]], b: list[tuple[float, float]]
) -> list[tuple[float, float]]:
    result: list[tuple[float, float]] = []
    ia, ib = iter(a), iter(b)
    sentinel = object()
    while True:
        va = next(ia, sentinel)
        vb = next(ib, sentinel)
        if va is sentinel and vb is sentinel:
            break
        if va is not sentinel:
            result.append(va)
        if vb is not sentinel:
            result.append(vb)
    return result


def _neighbours(key: tuple[int, int]) -> list[tuple[int, int]]:
    r, c = key
    return [
        (r + dr, c + dc) for dr in (-1, 0, 1) for dc in (-1, 0, 1) if (dr, dc) != (0, 0)
    ]


def _resolve_max_cell_size(
    key: tuple[int, int],
    config: GridConfig,
    cell_width: int,
    controls: list[GridControl],
    default_max_cell_size: int,
) -> int:
    lat = config.origin_lat + key[0] * cell_width * config.lat_res
    lon = config.origin_lon + key[1] * cell_width * config.lon_res
    for control in controls:
        if (
            control.box.lat_min <= lat <= control.box.lat_max
            and control.box.lon_min <= lon <= control.box.lon_max
        ):
            return int(control.relative_size * cell_width ** 2)
    return default_max_cell_size


def _build_supercells(
    cells: dict[tuple[int, int], list[tuple[float, float]]],
    max_cell_sizes: dict[tuple[int, int], int],
    max_merged: int,
) -> list[Cell]:
    result: list[Cell] = []
    used: set[tuple[int, int]] = set()

    # Pass 1: accept only full interior cells (at or above max_cell_size).
    # Partial boundary cells are held back so they can merge with
    # under-populated neighbours.
    for key in sorted(cells):
        if len(cells[key]) >= max_cell_sizes[key]:
            result.append(Cell(keys=[key], points=cells[key]))
            used.add(key)

    # Pass 2: merge remaining cells, processing the most under-populated
    # first so the neediest cells get first pick of merge partners.
    remaining = sorted(
        (k for k in cells if k not in used),
        key=lambda k: len(cells[k]),
    )

    failed: set[tuple[int, int]] = set()

    for key in remaining:
        if key in used:
            continue
        effective_max_cell_size = max_cell_sizes[key]
        merged_keys = [key]
        merged_pts = list(cells[key])
        used.add(key)

        changed = True
        while (
            changed
            and len(merged_keys) < max_merged
            and len(merged_pts) < effective_max_cell_size
        ):
            changed = False
            candidates: list[tuple[int, int]] = []
            for mk in merged_keys:
                for nk in _neighbours(mk):
                    if nk not in used and nk in cells and nk not in candidates:
                        candidates.append(nk)

            best: tuple[int, int] | None = None
            best_distance = float('inf')
            for nk in candidates:
                candidate_max_cell_size = min(effective_max_cell_size, max_cell_sizes[nk])
                new_total = len(merged_pts) + len(cells[nk])
                if new_total > candidate_max_cell_size:
                    continue
                distance = abs(new_total - candidate_max_cell_size)
                if distance < best_distance:
                    best_distance = distance
                    best = nk

            if best is not None:
                merged_keys.append(best)
                merged_pts = _interleave(merged_pts, cells[best])
                used.add(best)
                effective_max_cell_size = min(effective_max_cell_size, max_cell_sizes[best])
                changed = True

        if len(merged_pts) == effective_max_cell_size:
            result.append(Cell(keys=merged_keys, points=merged_pts))
        else:
            failed.update(merged_keys)

    # Release keys from failed supercells so absorption can reach them.
    used -= failed

    # Absorption passes: repeatedly try to fold remaining orphaned cells
    # into adjacent supercells that still have room.
    absorbed = True
    while absorbed:
        absorbed = False
        orphans = sorted(
            (k for k in cells if k not in used),
            key=lambda k: len(cells[k]),
        )
        for key in orphans:
            for sc in result:
                sc_min_max_cell_size = min(max_cell_sizes[k] for k in sc.keys)
                if len(sc.keys) >= max_merged:
                    continue
                if len(sc.points) + len(cells[key]) > min(sc_min_max_cell_size, max_cell_sizes[key]):
                    continue
                if any(nk in _neighbours(key) for nk in sc.keys):
                    sc.keys.append(key)
                    sc.points = _interleave(sc.points, cells[key])
                    used.add(key)
                    absorbed = True
                    break

    # Orphan merge: form new supercells from remaining orphans.
    # No lower bound — a small cell is better than discarded points.
    orphan_keys = sorted(
        (k for k in cells if k not in used),
        key=lambda k: len(cells[k]),
    )
    for key in orphan_keys:
        if key in used:
            continue
        effective_max_cell_size = max_cell_sizes[key]
        merged_keys = [key]
        merged_pts = list(cells[key])
        used.add(key)

        changed = True
        while (
            changed
            and len(merged_keys) < max_merged
            and len(merged_pts) < effective_max_cell_size
        ):
            changed = False
            candidates: list[tuple[int, int]] = []
            for mk in merged_keys:
                for nk in _neighbours(mk):
                    if nk not in used and nk in cells and nk not in candidates:
                        candidates.append(nk)

            best: tuple[int, int] | None = None
            best_distance = float('inf')
            for nk in candidates:
                candidate_max_cell_size = min(effective_max_cell_size, max_cell_sizes[nk])
                new_total = len(merged_pts) + len(cells[nk])
                if new_total > candidate_max_cell_size:
                    continue
                distance = abs(new_total - candidate_max_cell_size)
                if distance < best_distance:
                    best_distance = distance
                    best = nk

            if best is not None:
                merged_keys.append(best)
                merged_pts = _interleave(merged_pts, cells[best])
                used.add(best)
                effective_max_cell_size = min(effective_max_cell_size, max_cell_sizes[best])
                changed = True

        result.append(Cell(keys=merged_keys, points=merged_pts))

    return result


def plot_packed_cell(
    cell: Cell,
    config: GridConfig,
    cell_width: int,
    out_dir: Path,
    color: str = PACKED_CELL_COLORS[0],
) -> None:
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt

    uk_gdf = _load_uk_geometry()

    cell_w = config.lon_res * cell_width
    cell_h = config.lat_res * cell_width

    lats = [p[0] for p in cell.points]
    lons = [p[1] for p in cell.points]
    pad_lon = cell_w
    pad_lat = cell_h

    fig, ax = plt.subplots(1, 1, figsize=(6, 6))

    for row, col in cell.keys:
        x = config.origin_lon + col * cell_w
        y = config.origin_lat + row * cell_h
        ax.add_patch(
            mpatches.Rectangle(
                (x, y),
                cell_w,
                cell_h,
                facecolor=color,
                edgecolor='none',
                alpha=0.4,
            )
        )

    uk_gdf.boundary.plot(ax=ax, color='black', linewidth=0.5)

    ax.scatter(lons, lats, marker=MARKER_STYLE, s=MARKER_SIZE * 4, facecolors='black', edgecolors='none')

    ax.set_xlim(min(lons) - pad_lon, max(lons) + pad_lon)
    ax.set_ylim(min(lats) - pad_lat, max(lats) + pad_lat)
    ax.set_aspect('equal')

    col_step = config.lon_res * cell_width
    row_step = config.lat_res * cell_width

    ax.secondary_xaxis(
        'top',
        functions=(
            lambda lon: (lon - config.origin_lon) / col_step,
            lambda c: c * col_step + config.origin_lon,
        ),
    ).set_xlabel('Column index')

    ax.secondary_yaxis(
        'right',
        functions=(
            lambda lat: (lat - config.origin_lat) / row_step,
            lambda r: r * row_step + config.origin_lat,
        ),
    ).set_ylabel('Row index')

    label = _cell_label(cell)
    ax.set_title(f'{label} — remote-merged ({len(cell.keys)} fragments, {len(cell.points)} pts)')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f'{label}.{PLOT_FORMAT}'
    _save_figure(fig, out_path)
    print(f'  plot saved → {out_path}')
    plt.close(fig)


def _pack_undersized_cells(
    cells: list[Cell],
    max_cell_sizes: dict[tuple[int, int], int],
    min_cell_size: int,
    config: GridConfig,
    cell_width: int,
) -> list[Cell]:
    """Bin-pack undersized cells into larger ones, discarding any remainder."""
    undersized: list[Cell] = []
    kept: list[Cell] = []
    for cell in cells:
        if len(cell.points) < min_cell_size:
            undersized.append(cell)
        else:
            kept.append(cell)

    if not undersized:
        return cells

    # Sort largest first so we start bins with the biggest fragments.
    undersized.sort(key=lambda c: len(c.points), reverse=True)

    bins: list[Cell] = []
    bin_caps: list[int] = []

    for cell in undersized:
        cell_cap = min(max_cell_sizes[k] for k in cell.keys)

        # First-fit decreasing: try to add to an existing bin.
        best_idx: int | None = None
        best_remaining = float('inf')
        for i, b in enumerate(bins):
            effective_cap = min(bin_caps[i], cell_cap)
            remaining = effective_cap - len(b.points)
            if len(cell.points) <= remaining and remaining < best_remaining:
                best_idx = i
                best_remaining = remaining

        if best_idx is not None:
            bins[best_idx].keys.extend(cell.keys)
            bins[best_idx].points = _interleave(bins[best_idx].points, cell.points)
            bin_caps[best_idx] = min(bin_caps[best_idx], cell_cap)
        else:
            bins.append(Cell(keys=list(cell.keys), points=list(cell.points)))
            bin_caps.append(cell_cap)

    # Keep only bins that reached the minimum size.
    packed = [b for b in bins if len(b.points) >= min_cell_size]
    discarded = [b for b in bins if len(b.points) < min_cell_size]

    for b in packed:
        b.packed = True
        if REPORT_PACKED_CELLS:
            print(f'  {_cell_label(b)}: remote-merged {len(b.keys)} fragments ({len(b.points)} pts)')
    for b in discarded:
        if REPORT_PACKED_CELLS:
            print(f'  {_cell_label(b)}: discarded {len(b.keys)} fragments ({len(b.points)} pts)')

    return kept + packed


def generate_cells(
    points: list[tuple[float, float]],
    config: GridConfig,
    cell_width: int = NOMINAL_CELL_WIDTH,
    controls: list[GridControl] = GRID_CONTROLS,
    min_relative_size: float = MIN_RELATIVE_SIZE,
    max_relative_size: float = MAX_RELATIVE_SIZE,
) -> list[Cell]:
    coarse = _assign_coarse_cells(points, config, cell_width)
    for key in coarse:
        coarse[key] = _scatter_cell_points(coarse[key], key, config, cell_width)
    default_max_cell_size = int(max_relative_size * cell_width ** 2)
    max_cell_sizes = {
        key: _resolve_max_cell_size(key, config, cell_width, controls, default_max_cell_size)
        for key in coarse
    }
    cells = _build_supercells(coarse, max_cell_sizes, MAX_MERGED_CELLS)
    min_cell_size = int(min_relative_size * cell_width ** 2)
    return _pack_undersized_cells(cells, max_cell_sizes, min_cell_size, config, cell_width)


def _build_adjacency(cells: list[Cell]) -> list[set[int]]:
    key_to_cell: dict[tuple[int, int], int] = {}
    for i, cell in enumerate(cells):
        for k in cell.keys:
            key_to_cell[k] = i

    adj: list[set[int]] = [set() for _ in cells]
    for i, cell in enumerate(cells):
        for k in cell.keys:
            for nk in _neighbours(k):
                j = key_to_cell.get(nk)
                if j is not None and j != i:
                    adj[i].add(j)
    return adj


def color_cells(cells: list[Cell], initial_colors: int = 5) -> list[int]:
    adj = _build_adjacency(cells)
    n = len(cells)
    order = sorted(range(n), key=lambda i: len(adj[i]), reverse=True)
    num_colors = initial_colors

    while True:
        colors = [-1] * n
        ok = True
        for i in order:
            used = {colors[j] for j in adj[i] if colors[j] != -1}
            assigned = False
            for c in range(num_colors):
                if c not in used:
                    colors[i] = c
                    assigned = True
                    break
            if not assigned:
                ok = False
                break
        if ok:
            return colors
        num_colors += 1


def _pad_points(
    points: list[tuple[float, float]], max_cell_size: int
) -> list[tuple[float, float]]:
    n = len(points)
    if n == 0:
        return []
    if n >= max_cell_size:
        return points[:max_cell_size]
    repeats = math.ceil(max_cell_size / n)
    return (points * repeats)[:max_cell_size]


def _cell_label(cell: Cell) -> str:
    row, col = min(cell.keys)
    row_str = f'{row:02d}' if row >= 0 else f'-{abs(row):02d}'
    col_str = f'{col:02d}' if col >= 0 else f'-{abs(col):02d}'
    return f'cell_{row_str}_{col_str}'


def write_cells(cells: list[Cell], out_dir: Path, max_cell_size: int) -> None:
    if out_dir.exists():
        for f in out_dir.glob('*.csv'):
            f.unlink()
    out_dir.mkdir(parents=True, exist_ok=True)

    for cell in cells:
        cell_path = out_dir / f'{_cell_label(cell)}.csv'
        padded = _pad_points(cell.points, max_cell_size)
        with open(cell_path, 'w', newline='') as f:
            csv.writer(f).writerows(padded)


def write_samples(cells: list[Cell], out_dir: Path, max_cell_size: int) -> None:
    if out_dir.exists():
        for f in out_dir.glob('*.csv'):
            f.unlink()
    out_dir.mkdir(parents=True, exist_ok=True)

    padded_cells = [_pad_points(cell.points, max_cell_size) for cell in cells]

    for j in range(max_cell_size):
        rows = [padded[j] for padded in padded_cells if j < len(padded)]
        if not rows:
            continue
        sample_path = out_dir / f'sample_{j:03d}.csv'
        with open(sample_path, 'w', newline='') as f:
            csv.writer(f).writerows(rows)


def main() -> None:
    lat_res = float(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_LAT_RESOLUTION
    lon_res = float(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_LON_RESOLUTION
    cell_width = int(sys.argv[3]) if len(sys.argv) > 3 else NOMINAL_CELL_WIDTH

    config = GridConfig(lat_res=lat_res, lon_res=lon_res)
    grid_points = generate_uk_grid(config)

    max_cell_size = int(MAX_RELATIVE_SIZE * cell_width ** 2)
    cells = generate_cells(grid_points, config, cell_width)

    total_points = sum(len(c.points) for c in cells)
    sizes = [len(c.points) for c in cells]
    print(
        f'Generated {len(cells)} cells (cell_width={cell_width}) '
        f'containing {total_points} points '
        f'[min={min(sizes)}, max={max(sizes)}]'
    )

    if WRITE_CELLS:
        cells_dir = Path(CELLS_OUTPUT_DIR)
        write_cells(cells, cells_dir, max_cell_size)
        print(f'Cell files written → {cells_dir}/')

    if WRITE_SAMPLES:
        samples_dir = Path(SAMPLES_OUTPUT_DIR)
        write_samples(cells, samples_dir, max_cell_size)
        print(f'Sample files written → {samples_dir}/')

    cell_colors = color_cells(cells)
    num_graph_colors = max(cell_colors) + 1

    # Build palette: graph-colouring colours first, then one unique colour per packed cell.
    palette: list[str] = list(CELL_COLOR_POOL)
    while len(palette) < num_graph_colors:
        palette.append(palette[len(palette) % len(CELL_COLOR_POOL)])

    packed_indexed = [(i, c) for i, c in enumerate(cells) if c.packed]
    if COLOR_PACKED_CELLS:
        packed_colors = [PACKED_CELL_COLORS[j % len(PACKED_CELL_COLORS)] for j in range(len(packed_indexed))]
    else:
        packed_colors = ['#aaaaaa'] * len(packed_indexed)

    for packed_idx, ((i, _), color) in enumerate(zip(packed_indexed, packed_colors)):
        cell_colors[i] = num_graph_colors + packed_idx
        palette.append(color)

    print(f'Coloured cells with {num_graph_colors} graph colours + {len(packed_indexed)} packed-cell colours')

    max_plottable = 200000
    if total_points <= max_plottable:
        main_plot_dir = Path(MAIN_PLOT_DIR)
        plot_cells(cells, cell_colors, grid_points, config, cell_width, main_plot_dir, palette)
        if PLOT_SAMPLES:
            plot_samples(cells, cell_colors, config, cell_width, max_cell_size)
        if PLOT_PACKED_CELLS and packed_indexed:
            packed_plot_dir = Path(PACKED_CELLS_PLOT_DIR)
            for (_, cell), color in zip(packed_indexed, packed_colors):
                plot_packed_cell(cell, config, cell_width, packed_plot_dir, color)
        if PLOT_SAMPLE_DETAILS:
            plot_sample_details(cells, cell_colors, palette, config, cell_width, max_cell_size, main_plot_dir)
    else:
        print(f'Skipping plots ({total_points} points exceeds {max_plottable})')


if __name__ == '__main__':
    main()
