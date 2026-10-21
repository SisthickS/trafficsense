"""Registry of verified dataset sources: pinned URLs, checksums and expected dataset shape.

Every remote file is pinned to an immutable location (Zenodo record, Hugging Face revision, GitHub
commit) together with a published checksum, so a download is reproducible and verifiable.
"""
from __future__ import annotations

from dataclasses import dataclass, field

DCRNN_COMMIT = "602afd9d767d3aa1c9b3eac51710d6aeee12c227"
DCRNN_SENSOR_GRAPH = f"https://raw.githubusercontent.com/liyaguang/DCRNN/{DCRNN_COMMIT}/data/sensor_graph"
ZENODO_RECORD = "5724362"
ZENODO_FILES = f"https://zenodo.org/api/records/{ZENODO_RECORD}/files"
HF_REPO = "jimmygao3218/PEMSBAY"
HF_REVISION = "c858397fb381d82c3d55ad0a81beb51a9bb9bdaa"

DCRNN_CITATION = (
    "Li, Y., Yu, R., Shahabi, C., Liu, Y. (2018). Diffusion Convolutional Recurrent Neural Network: "
    "Data-Driven Traffic Forecasting. ICLR 2018. Data: Caltrans PeMS (PEMS-BAY), LA Metro loop "
    "detectors (METR-LA)."
)

ROLE_READINGS = "readings"          # wide CSV: timestamp + one speed column per sensor
ROLE_READINGS_H5 = "readings_h5"    # original DCRNN HDF5 (fallback source)
ROLE_ADJACENCY = "adjacency"        # pickled (sensor_ids, id_to_index, adj_mx)
ROLE_LOCATIONS = "sensor_locations" # sensor_id, latitude, longitude


@dataclass(frozen=True)
class RemoteFile:
    name: str
    url: str
    role: str
    size: int | None = None
    md5: str | None = None
    sha256: str | None = None


@dataclass(frozen=True)
class DatasetSpec:
    """What a correct copy of the dataset looks like (used by the validator)."""
    dataset: str
    n_sensors: int
    expected_rows: int
    interval_seconds: int
    local_timezone: str
    timestamp_format: str
    lat_range: tuple[float, float]
    lon_range: tuple[float, float]
    max_plausible_speed: float = 100.0
    locations_has_header: bool = False


@dataclass(frozen=True)
class DatasetSource:
    dataset: str
    source: str
    description: str
    license: str
    landing_page: str
    files: tuple[RemoteFile, ...]
    citation: str = DCRNN_CITATION
    notes: tuple[str, ...] = field(default_factory=tuple)

    def file_for(self, role: str) -> RemoteFile | None:
        return next((f for f in self.files if f.role == role), None)


SPECS: dict[str, DatasetSpec] = {
    "pems-bay": DatasetSpec(
        dataset="pems-bay",
        n_sensors=325,
        expected_rows=52_116,
        interval_seconds=300,
        local_timezone="America/Los_Angeles",
        timestamp_format="yyyy-MM-dd HH:mm:ss",
        lat_range=(36.9, 38.2),
        lon_range=(-122.6, -121.5),
    ),
    "metr-la": DatasetSpec(
        dataset="metr-la",
        n_sensors=207,
        expected_rows=34_272,
        interval_seconds=300,
        local_timezone="America/Los_Angeles",
        timestamp_format="yyyy-MM-dd HH:mm:ss",
        lat_range=(33.6, 34.5),
        lon_range=(-118.8, -117.9),
        locations_has_header=True,
    ),
}

SOURCES: dict[tuple[str, str], DatasetSource] = {
    ("pems-bay", "zenodo"): DatasetSource(
        dataset="pems-bay",
        source="zenodo",
        description="PEMS-BAY speed readings (DCRNN benchmark) exported to CSV, 325 Bay Area sensors, 5-min.",
        license="CC-BY-4.0 (Zenodo record); original data Caltrans PeMS",
        landing_page=f"https://doi.org/10.5281/zenodo.{ZENODO_RECORD}",
        files=(
            RemoteFile("PEMS-BAY.csv", f"{ZENODO_FILES}/PEMS-BAY.csv/content", ROLE_READINGS,
                       size=85_718_439, md5="c8dea58987a5882e946217c22fdb8256"),
            RemoteFile("adj_mx_PEMS-BAY.pkl", f"{ZENODO_FILES}/adj_mx_PEMS-BAY.pkl/content", ROLE_ADJACENCY,
                       size=1_681_480, md5="55d25daceac847312b7748c59ded6f77"),
            RemoteFile("graph_sensor_locations_bay.csv", f"{DCRNN_SENSOR_GRAPH}/graph_sensor_locations_bay.csv",
                       ROLE_LOCATIONS, size=9_750,
                       sha256="276ee01059610774d4e59572507f7e32eaac21f1f5882fcd9e3d7d426a4b7a6c"),
        ),
    ),
    ("pems-bay", "huggingface"): DatasetSource(
        dataset="pems-bay",
        source="huggingface",
        description="Original DCRNN PEMS-BAY HDF5 mirrored on Hugging Face (fallback if Zenodo is unavailable).",
        license="CC-BY-4.0 (HF repo card); original data Caltrans PeMS",
        landing_page=f"https://huggingface.co/datasets/{HF_REPO}",
        files=(
            RemoteFile("PEMS-BAY.h5", f"https://huggingface.co/datasets/{HF_REPO}/resolve/{HF_REVISION}/PEMS-BAY.h5",
                       ROLE_READINGS_H5, size=135_930_936,
                       sha256="65d69fb0a2323dba9867179eb7af47c8b814186bc459ff0a4937d21614153c8f"),
            RemoteFile("adj_mx_bay.pkl", f"https://huggingface.co/datasets/{HF_REPO}/resolve/{HF_REVISION}/adj_mx_bay.pkl",
                       ROLE_ADJACENCY, size=1_681_480,
                       sha256="116275f5704d0d492018e14c047f7ae5004385b450aa907f88424055a4a97370"),
            RemoteFile("graph_sensor_locations_bay.csv", f"{DCRNN_SENSOR_GRAPH}/graph_sensor_locations_bay.csv",
                       ROLE_LOCATIONS, size=9_750,
                       sha256="276ee01059610774d4e59572507f7e32eaac21f1f5882fcd9e3d7d426a4b7a6c"),
        ),
        notes=("HDF5 is exported once to a Spark-readable wide CSV (PEMS-BAY.csv); the .h5 stays untouched.",),
    ),
    ("metr-la", "zenodo"): DatasetSource(
        dataset="metr-la",
        source="zenodo",
        description="METR-LA speed readings (DCRNN benchmark) exported to CSV, 207 LA sensors, 5-min.",
        license="CC-BY-4.0 (Zenodo record); original data LA Metro",
        landing_page=f"https://doi.org/10.5281/zenodo.{ZENODO_RECORD}",
        files=(
            RemoteFile("METR-LA.csv", f"{ZENODO_FILES}/METR-LA.csv/content", ROLE_READINGS,
                       size=72_467_662, md5="7c463fcbd28ecc6c9cd93e1ca120aaaa"),
            RemoteFile("adj_mx_METR-LA.pkl", f"{ZENODO_FILES}/adj_mx_METR-LA.pkl/content", ROLE_ADJACENCY,
                       size=680_459, md5="55996dfce3e39cc981e6e42e3f427da8"),
            RemoteFile("graph_sensor_locations.csv", f"{DCRNN_SENSOR_GRAPH}/graph_sensor_locations.csv",
                       ROLE_LOCATIONS, size=6_341,
                       sha256="eb8ea96e07358b45d0e4ba3b89c2673fa20c54af50150249e627389e749ade6f"),
        ),
    ),
}

# Considered but intentionally not used as a raw source (documented in the validation report/README).
REJECTED_SOURCES = {
    "huggingface:witgaw/PEMS-BAY": (
        "Pre-windowed ('augmented') samples: 12 input + 12 target steps per row, each reading duplicated "
        "~24x with normalised time-of-day features. Not raw data; TrafficSense builds these windows itself "
        "with Spark window functions in Phase 5."
    ),
}


def get_source(dataset: str, source: str) -> DatasetSource:
    try:
        return SOURCES[(dataset, source)]
    except KeyError:
        options = ", ".join(f"{d}/{s}" for d, s in SOURCES)
        raise SystemExit(f"Unknown dataset/source '{dataset}/{source}'. Options: {options}") from None
