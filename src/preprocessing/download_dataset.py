"""Acquire a traffic dataset into data/raw/<dataset>/ with checksum verification.

Usage (from the project root, with ~/.trafficsense_env.sh sourced and the venv active):
    python -m src.preprocessing.download_dataset --inspect                 # check sources, no download
    python -m src.preprocessing.download_dataset                           # PEMS-BAY from Zenodo
    python -m src.preprocessing.download_dataset --source huggingface      # PEMS-BAY fallback (HDF5)
    python -m src.preprocessing.download_dataset --dataset metr-la         # verified alternative

Raw-data contract:
  * Files are verified against pinned checksums before they are accepted.
  * Accepted files are made read-only (0444) and recorded in MANIFEST.json with md5 + sha256.
  * An existing raw file whose checksum no longer matches is never silently replaced (needs --force).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import stat
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from src.common.paths import PROJECT_ROOT, RAW_DIR, raw_dataset_dir
from src.preprocessing.sources import (
    REJECTED_SOURCES,
    ROLE_ADJACENCY,
    ROLE_LOCATIONS,
    ROLE_READINGS,
    ROLE_READINGS_H5,
    DatasetSource,
    RemoteFile,
    get_source,
)

log = logging.getLogger("download_dataset")
USER_AGENT = "TrafficSense/0.2 (university big-data project)"
CHUNK = 1 << 20
MANIFEST_NAME = "MANIFEST.json"


def file_hashes(path: Path) -> dict[str, str]:
    md5, sha = hashlib.md5(), hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(CHUNK), b""):
            md5.update(block)
            sha.update(block)
    return {"md5": md5.hexdigest(), "sha256": sha.hexdigest()}


def matches_pin(hashes: dict[str, str], size: int, rf: RemoteFile) -> bool:
    if rf.size is not None and size != rf.size:
        return False
    if rf.sha256 and hashes["sha256"] != rf.sha256:
        return False
    if rf.md5 and hashes["md5"] != rf.md5:
        return False
    return True


def _open(url: str, timeout: int, method: str = "GET"):
    req = urllib.request.Request(url, method=method, headers={"User-Agent": USER_AGENT})
    return urllib.request.urlopen(req, timeout=timeout)


def download(rf: RemoteFile, dest: Path, timeout: int, retries: int = 3) -> None:
    part = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, retries + 1):
        try:
            with _open(rf.url, timeout) as resp, open(part, "wb") as out:
                total = int(resp.headers.get("Content-Length") or rf.size or 0)
                done, next_report, started = 0, 0.1, time.time()
                for block in iter(lambda: resp.read(CHUNK), b""):
                    out.write(block)
                    done += len(block)
                    if total and done / total >= next_report:
                        rate = done / max(time.time() - started, 1e-6) / 1e6
                        log.info("  %-32s %5.1f%%  %7.1f MB  %.1f MB/s", rf.name, 100 * done / total, done / 1e6, rate)
                        next_report += 0.1
            part.replace(dest)
            return
        except Exception as exc:  # network errors are retried; anything else surfaces after retries
            log.warning("  attempt %d/%d failed for %s: %s", attempt, retries, rf.name, exc)
            part.unlink(missing_ok=True)
            if attempt == retries:
                raise
            time.sleep(2 * attempt)


def make_read_only(path: Path) -> None:
    path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)


def load_verified_manifest(ds_dir: Path) -> dict:
    """Load MANIFEST.json and abort unless every listed raw file still matches its recorded sha256."""
    manifest_path = ds_dir / MANIFEST_NAME
    if not manifest_path.exists():
        raise SystemExit(f"{manifest_path} not found - run download_dataset first.")
    manifest = json.loads(manifest_path.read_text())
    for name, meta in manifest["files"].items():
        path = ds_dir / name
        if not path.exists() or file_hashes(path)["sha256"] != meta["sha256"]:
            raise SystemExit(f"Raw file {path} is missing or does not match MANIFEST.json - raw data was modified.")
    return manifest


def make_writable(path: Path) -> None:
    path.chmod(path.stat().st_mode | stat.S_IWUSR)


def acquire_file(rf: RemoteFile, target_dir: Path, timeout: int, force: bool) -> dict:
    dest = target_dir / rf.name
    if dest.exists():
        hashes = file_hashes(dest)
        if matches_pin(hashes, dest.stat().st_size, rf):
            log.info("[skip] %s already present and verified", rf.name)
            return {**hashes, "size": dest.stat().st_size}
        if not force:
            raise SystemExit(
                f"[abort] {dest} exists but does not match the pinned checksum. Raw data must not be "
                "modified. Inspect it, then re-run with --force to replace it."
            )
        log.warning("[force] replacing modified raw file %s", dest)
        make_writable(dest)
        dest.unlink()

    log.info("[get ] %s <- %s", rf.name, rf.url)
    download(rf, dest, timeout)
    hashes = file_hashes(dest)
    size = dest.stat().st_size
    if not matches_pin(hashes, size, rf):
        bad = dest.with_suffix(dest.suffix + ".rejected")
        dest.replace(bad)
        raise SystemExit(f"[fail] checksum/size mismatch for {rf.name} (kept as {bad.name} for inspection)")
    make_read_only(dest)
    log.info("[ ok ] %s verified (%s bytes, sha256 %s...)", rf.name, f"{size:,}", hashes["sha256"][:12])
    return {**hashes, "size": size}


def export_h5_to_csv(h5_path: Path, csv_path: Path) -> dict:
    """Convert the DCRNN HDF5 (wide DataFrame) into the same wide CSV layout as the Zenodo export."""
    import pandas as pd  # local import: only needed for the HF fallback

    if csv_path.exists():
        make_writable(csv_path)
        csv_path.unlink()
    df = pd.read_hdf(h5_path)
    log.info("[conv] %s -> %s  (%d rows x %d sensors)", h5_path.name, csv_path.name, *df.shape)
    tmp = csv_path.with_suffix(".csv.part")
    df.to_csv(tmp, index_label="", date_format="%Y-%m-%d %H:%M:%S")
    tmp.replace(csv_path)
    make_read_only(csv_path)
    hashes = file_hashes(csv_path)
    return {**hashes, "size": csv_path.stat().st_size}


def write_manifest(src: DatasetSource, target_dir: Path, file_meta: dict[str, dict]) -> Path:
    def name_for(role: str) -> str | None:
        return next((n for n, m in file_meta.items() if m["role"] == role), None)

    manifest = {
        "dataset": src.dataset,
        "source": src.source,
        "description": src.description,
        "license": src.license,
        "landing_page": src.landing_page,
        "citation": src.citation,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tool": "src/preprocessing/download_dataset.py",
        "readings_csv": name_for(ROLE_READINGS),
        "locations_csv": name_for(ROLE_LOCATIONS),
        "adjacency_pkl": name_for(ROLE_ADJACENCY),
        "files": file_meta,
        "notes": list(src.notes),
    }
    path = target_dir / MANIFEST_NAME
    if path.exists():
        previous = json.loads(path.read_text())
        same_files = {n: m["sha256"] for n, m in previous.get("files", {}).items()} == \
                     {n: m["sha256"] for n, m in file_meta.items()}
        if same_files and previous.get("retrieved_at_utc"):
            manifest["retrieved_at_utc"] = previous["retrieved_at_utc"]
        make_writable(path)
    path.write_text(json.dumps(manifest, indent=2) + "\n")
    return path


def run_download(src: DatasetSource, raw_dir: Path, timeout: int, force: bool) -> Path:
    target_dir = raw_dataset_dir(src.dataset, raw_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    log.info("Dataset %s from %s -> %s", src.dataset, src.source, target_dir)

    file_meta: dict[str, dict] = {}
    for rf in src.files:
        info = acquire_file(rf, target_dir, timeout, force)
        file_meta[rf.name] = {"role": rf.role, "url": rf.url, "derived_from": None, **info}

    h5 = src.file_for(ROLE_READINGS_H5)
    if h5 is not None:
        csv_name = h5.name.rsplit(".", 1)[0] + ".csv"
        info = export_h5_to_csv(target_dir / h5.name, target_dir / csv_name)
        file_meta[csv_name] = {"role": ROLE_READINGS, "url": None, "derived_from": h5.name, **info}

    manifest = write_manifest(src, target_dir, file_meta)
    log.info("Manifest written: %s", manifest.relative_to(PROJECT_ROOT) if manifest.is_relative_to(PROJECT_ROOT) else manifest)
    return target_dir


def _remote_metadata(src: DatasetSource, timeout: int) -> dict[str, dict]:
    """Fetch live size/checksum metadata from the hosting service for comparison with the pins."""
    meta: dict[str, dict] = {}
    try:
        if src.source == "zenodo":
            record_url = src.files[0].url.split("/files/")[0]
            with _open(record_url, timeout) as resp:
                record = json.load(resp)
            for f in record.get("files", []):
                algo, _, value = f.get("checksum", "").partition(":")
                meta[f["key"]] = {"size": f.get("size"), algo: value}
        elif src.source == "huggingface":
            repo_rev = src.files[0].url.split("/datasets/")[1].split("/resolve/")
            api = f"https://huggingface.co/api/datasets/{repo_rev[0]}/tree/{repo_rev[1].split('/')[0]}"
            with _open(api, timeout) as resp:
                for f in json.load(resp):
                    lfs = f.get("lfs") or {}
                    meta[f["path"]] = {"size": f.get("size"), "sha256": lfs.get("oid")}
    except Exception as exc:
        log.warning("Could not query %s API: %s", src.source, exc)
    for rf in src.files:
        if rf.name not in meta:
            try:
                with _open(rf.url, timeout, method="HEAD") as resp:
                    meta[rf.name] = {"size": int(resp.headers.get("Content-Length", 0)) or None}
            except Exception as exc:
                meta[rf.name] = {"error": str(exc)}
    return meta


def run_inspect(src: DatasetSource, raw_dir: Path, timeout: int) -> int:
    target_dir = raw_dataset_dir(src.dataset, raw_dir)
    print(f"\nDataset : {src.dataset}\nSource  : {src.source}  ({src.landing_page})")
    print(f"About   : {src.description}\nLicense : {src.license}\nTarget  : {target_dir}\n")
    remote = _remote_metadata(src, timeout)
    problems = 0
    header = f"{'file':34} {'role':17} {'pinned size':>12} {'remote size':>12} {'remote checksum':>16} {'local':>10}"
    print(header + "\n" + "-" * len(header))
    for rf in src.files:
        r = remote.get(rf.name, {})
        if "error" in r:
            remote_ck, problems = "UNREACHABLE", problems + 1
        elif rf.md5 and r.get("md5"):
            remote_ck = "md5 match" if r["md5"] == rf.md5 else "md5 DIFFERS"
        elif rf.sha256 and r.get("sha256"):
            remote_ck = "sha256 match" if r["sha256"] == rf.sha256 else "sha256 DIFFERS"
        else:
            remote_ck = "n/a (verify)"
        if "DIFFERS" in remote_ck or (r.get("size") and rf.size and r["size"] != rf.size):
            problems += 1
        local = target_dir / rf.name
        if local.exists():
            local_status = "verified" if matches_pin(file_hashes(local), local.stat().st_size, rf) else "MODIFIED"
        else:
            local_status = "absent"
        print(f"{rf.name:34} {rf.role:17} {rf.size or 0:>12,} {r.get('size') or 0:>12,} {remote_ck:>16} {local_status:>10}")
    print("\nNot used as raw sources:")
    for name, why in REJECTED_SOURCES.items():
        print(f"  - {name}: {why}")
    print(f"\nInspection {'OK' if problems == 0 else f'found {problems} problem(s)'}.")
    return 0 if problems == 0 else 1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dataset", default="pems-bay", choices=["pems-bay", "metr-la"])
    p.add_argument("--source", default="zenodo", choices=["zenodo", "huggingface"])
    p.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    p.add_argument("--inspect", action="store_true", help="query sources and local state; download nothing")
    p.add_argument("--force", action="store_true", help="replace raw files whose checksum no longer matches")
    p.add_argument("--timeout", type=int, default=60)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    args = parse_args(argv)
    src = get_source(args.dataset, args.source)
    if args.inspect:
        return run_inspect(src, args.raw_dir, args.timeout)
    run_download(src, args.raw_dir, args.timeout, args.force)
    return 0


if __name__ == "__main__":
    os.umask(0o022)
    sys.exit(main())
