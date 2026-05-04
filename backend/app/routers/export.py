import io
import uuid
from pathlib import Path
from typing import Iterator

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import ImportSession

router = APIRouter(prefix="/api", tags=["export"])


def _read_source_file(file_path: Path) -> pd.DataFrame:
    ext = file_path.suffix.lower()

    if ext == ".csv":
        return pd.read_csv(file_path, keep_default_na=True)

    if ext in {".xls", ".xlsx"}:
        return pd.read_excel(file_path)

    raise ValueError(f"Unsupported stored file type: {ext}")


def _build_clean_export(
    df: pd.DataFrame,
    confirmed_mappings: list[dict],
    all_error_indices: list[int],
) -> pd.DataFrame:
    df_working = df.reset_index(drop=True)

    clean_df = df_working.drop(
        index=set(all_error_indices),
        errors="ignore",
    )

    rename_map = {
        item["original"]: item["canonical"]
        for item in confirmed_mappings
        if item.get("original") and item.get("canonical")
    }

    return clean_df.rename(columns=rename_map)

def _stream_csv(df, chunk_size: int = 1000):
    import io

    buffer = io.StringIO()

    # write header once
    df.head(0).to_csv(buffer, index=False)
    yield buffer.getvalue().encode()
    buffer.seek(0)
    buffer.truncate(0)

    total_rows = len(df)

    for start in range(0, total_rows, chunk_size):
        chunk = df.iloc[start:start + chunk_size]

        chunk.to_csv(buffer, index=False, header=False)

        yield buffer.getvalue().encode()

        buffer.seek(0)
        buffer.truncate(0)


@router.get("/export/{file_id}")
async def export_clean_file(
    file_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ImportSession).where(ImportSession.id == file_id)
    )
    import_session = result.scalar_one_or_none()

    if import_session is None:
        raise HTTPException(status_code=404, detail="Import session not found.")

    if import_session.state != "VALIDATED":
        raise HTTPException(
            status_code=400,
            detail="Validation incomplete - cannot export yet.",
        )

    metadata_json = import_session.metadata_json or {}
    validation_summary = metadata_json.get("validation_summary")
    confirmed_mappings = metadata_json.get("confirmed_mappings")
    all_error_indices = metadata_json.get("all_error_indices")

    if not validation_summary:
        raise HTTPException(
            status_code=400,
            detail="Validation summary missing - export blocked.",
        )

    if confirmed_mappings is None:
        raise HTTPException(
            status_code=400,
            detail="Confirmed mappings missing - export blocked.",
        )

    if all_error_indices is None:
        raise HTTPException(
            status_code=400,
            detail="Validation row index list missing - export blocked.",
        )

    file_path = Path(import_session.stored_path)

    if not file_path.exists():
        raise HTTPException(
            status_code=410,
            detail="Stored upload is gone - cannot export.",
        )

    try:
        df = await run_in_threadpool(_read_source_file, file_path)
        clean_df = await run_in_threadpool(
            _build_clean_export,
            df,
            confirmed_mappings,
            all_error_indices,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Export failed while reading source data: {str(exc)}",
        )

    if clean_df.empty:
        raise HTTPException(
            status_code=400,
            detail="No clean rows left to export.",
        )

    import_session.state = "EXPORTED"

    try:
        await db.commit()
    except Exception as exc:
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Export state update failed: {str(exc)}",
        )

    filename = f"datagraft_clean_{file_id}.csv"

    return StreamingResponse(
        _stream_csv(clean_df),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        },
    )