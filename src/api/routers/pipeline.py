"""Pipeline run history (`pipeline_runs`): analytics questions, training, evaluation, streaming checks, data loads - read-only."""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Path, Query

from src.api.deps import Paging, Reader
from src.api.routers import ERRORS
from src.api.schemas import PageOut
from src.api.serialize import public
from src.mongo.errors import NotFoundError

router = APIRouter(prefix="/api/v1/pipeline", tags=["pipeline"], responses={k: ERRORS[k] for k in (422, 503)})


@router.get("/status", summary="Latest run and failure count per kind of pipeline step")
def status(reader: Reader):
    return reader.latest_run_per_kind()


@router.get("/runs", summary="Run history, newest first")
def list_runs(reader: Reader, page: Paging,
              kind: Annotated[str | None, Query(pattern=r"^[a-z_]{1,32}$")] = None,
              status: Annotated[str | None, Query(pattern="^(succeeded|failed|running)$")] = None):
    items, total = reader.list_runs(kind, status, page.limit, page.offset)
    return PageOut[dict](items=[public(d) for d in items], total=total, limit=page.limit, offset=page.offset)


@router.get("/runs/{run_id}", responses={404: ERRORS[404]})
def get_run(run_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9_.:-]{1,80}$")], reader: Reader):
    doc = reader.get_run(run_id)
    if doc is None:
        raise NotFoundError("pipeline run", run_id)
    return public(doc)
