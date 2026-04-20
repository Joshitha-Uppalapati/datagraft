import uuid
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import ImportSession
from app.services.detector import DetectorService

router = APIRouter(prefix="/api", tags=["detect"])


def _read_csv(file_path: Path) -> pd.DataFrame:
    return pd.read_csv(file_path)


@router.get("/detect/{file_id}")
async def detect_columns(
    file_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> dict:
    result = await db.execute(
        select(ImportSession).where(ImportSession.id == file_id)
    )
    import_session = result.scalar_one_or_none()

    if import_session is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Import session not found.",
        )

    file_path = Path(import_session.stored_path)

    if not file_path.exists():
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="Uploaded file is no longer available on disk.",
        )

    try:
        df = await run_in_threadpool(_read_csv, file_path)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unable to read stored CSV file: {str(exc)}",
        )

    detector = DetectorService()
    columns = await run_in_threadpool(detector.detect_dataframe, df)

    existing_metadata = import_session.metadata_json or {}
    updated_metadata = {
        **existing_metadata,
        "detected_schema": columns,
    }

    import_session.state = "DETECTED"
    import_session.metadata_json = updated_metadata

    try:
        await db.commit()
    except Exception as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to persist detection results: {str(exc)}",
        )

    return {"columns": columns}