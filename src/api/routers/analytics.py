"""Spark analytics results Q1-Q18 (stored in `analysis_results`): findings, result tables and figures - read-only."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Path as PathParam
from fastapi.responses import FileResponse

from src.api.deps import Paging, Reader, Settings
from src.api.routers import ERRORS
from src.api.serialize import public
from src.mongo.errors import NotFoundError

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics"], responses={k: ERRORS[k] for k in (422, 503)})
Qid = Annotated[str, PathParam(pattern=r"^q([1-9]|1[0-8])$", description="q1 ... q18")]
FIGURE_RE = re.compile(r"^[A-Za-z0-9_.-]+\.png$")


def _question(doc: dict) -> dict:
    out = public(doc)
    out["figure_urls"] = [f"/api/v1/analytics/figures/{f}" for f in doc.get("figures", [])]
    return out


@router.get("/questions", summary="The 18 analytical questions with key metrics")
def list_questions(reader: Reader):
    return [{**{k: d[k] for k in ("qid", "number", "title", "question", "metrics", "generated_at")}, "findings": len(d["findings"]),
             "tables": d["tables"], "figures": d["figures"]} for d in reader.list_questions()]


@router.get("/questions/{qid}", responses={404: ERRORS[404]}, summary="One question: findings, implications, limitations, metrics")
def get_question(qid: Qid, reader: Reader):
    doc = reader.get_question(qid)
    if doc is None:
        raise NotFoundError("analysis", qid)
    return _question(doc)


@router.get("/questions/{qid}/tables", responses={404: ERRORS[404]}, summary="Result tables of a question (names, columns, row counts)")
def list_tables(qid: Qid, reader: Reader):
    tables = reader.list_tables(qid)
    if not tables:
        raise NotFoundError("analysis", qid)
    return [{"name": t["name"], "columns": t["columns"], "n_rows": t["n_rows"]} for t in tables]


@router.get("/questions/{qid}/tables/{name}", responses={404: ERRORS[404]}, summary="Rows of one result table (paginated)")
def get_table(qid: Qid, name: Annotated[str, PathParam(pattern=r"^[a-z0-9_]{1,64}$")], reader: Reader, page: Paging):
    doc = reader.get_table(qid, name, page.limit, page.offset)
    if doc is None:
        raise NotFoundError("table", f"{qid}/{name}")
    return {"qid": qid, "name": name, "columns": doc["columns"], "n_rows": doc["n_rows"], "limit": page.limit, "offset": page.offset,
            "rows": doc["rows"]}


@router.get("/figures/{filename}", responses={404: ERRORS[404]}, response_class=FileResponse, summary="A chart (PNG) produced by the analytics jobs")
def get_figure(filename: Annotated[str, PathParam(max_length=96)], settings: Settings):
    base = (Path(settings.reports_dir) / "figures" / "analytics").resolve()
    path = (base / filename).resolve()
    if not FIGURE_RE.match(filename) or path.parent != base or not path.is_file():      # no traversal, PNG only
        raise NotFoundError("figure", filename)
    return FileResponse(path, media_type="image/png", headers={"Cache-Control": "public, max-age=300"})
