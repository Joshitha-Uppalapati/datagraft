import io
import uuid
from typing import Any, Iterable

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import ImportSession

router = APIRouter(prefix="/api", tags=["export"])


def _dataframe_to_csv_stream(df: pd.DataFrame) -> Iterable[bytes]:
    """
    Stream DataFrame as CSV in chunks instead of building a giant string.

    Why: Returning bytes means constructing the full CSV in memory first.
    That works for small files, but explodes for large datasets (100MB+).
    This approach writes progressively and yields chunks, which is safer
    under memory pressure and aligns with how real data pipelines behave.
    """
    buffer = io.StringIO()

    # write header once
    df.iloc[:0].to_csv(buffer, index=False)
    yield buffer.getvalue().encode()
    buffer.seek(0)
    buffer.truncate(0)

    # stream row batches
    chunk_size = 1000
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start:start + chunk_size]
        chunk.to_csv(buffer, index=False, header=False)
        yield buffer.getvalue().encode()
        buffer.seek(0)
        buffer.truncate(0)


@router.get("/export/{file_id}")
async def export_clean_csv(
    file_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    # fetch session
    result = await db.execute(
        select(ImportSession).where(ImportSession.id == file_id)
    )
    import_session = result.scalar_one_or_none()

    if not import_session:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Import session not found.",
        )

    if import_session.state != "VALIDATED":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Run validation before exporting.",
        )

    metadata = import_session.metadata_json or {}
    validation_errors = metadata.get("validation_errors", [])
    confirmed_mappings = metadata.get("confirmed_mappings", [])

    # load dataframe
    try:
        df = await run_in_threadpool(
            pd.read_csv,
            import_session.stored_path,
            keep_default_na=True,
        )
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to read stored file: {str(e)}",
        )

    # compute error rows 
    error_indices = {
        err["row_index"]
        for err in validation_errors
        if "row_index" in err
    }

    # filter clean rows
    clean_df = await run_in_threadpool(
        lambda: df.loc[~df.index.isin(error_indices)]
    )

    if clean_df.empty:
        raise HTTPException(
            status_code=400,
            detail="No clean rows available for export.",
        )

    # rename columns
    rename_map = {
        item["original"]: item["canonical"]
        for item in confirmed_mappings
    }

    clean_df = await run_in_threadpool(
        lambda: clean_df.rename(columns=rename_map)
    )

    # streaming response 
    response = StreamingResponse(
        _dataframe_to_csv_stream(clean_df),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=datagraft_clean_{file_id}.csv"
        },
    )

    # update state
    import_session.state = "EXPORTED"
    try:
        await db.commit()
    except Exception:
        await db.rollback()

    return response