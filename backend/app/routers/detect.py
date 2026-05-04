import uuid
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import ImportSession
from app.services.detector import DetectorService

router = APIRouter(prefix="/api", tags=["detection"])


def _read_file(file_path: Path) -> pd.DataFrame:
    if file_path.suffix.lower() == ".csv":
        return pd.read_csv(file_path)
    return pd.read_excel(file_path)


@router.get("/detect/{file_id}")
async def detect_columns(
    file_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    # Fetch session
    result = await db.execute(
        select(ImportSession).where(ImportSession.id == file_id)
    )
    import_session = result.scalar_one_or_none()

    if import_session is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Import session not found.",
        )

    # State guard
    if import_session.state != "UPLOADED":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid state '{import_session.state}'. Detection requires 'UPLOADED'.",
        )

    file_path = Path(import_session.stored_path)

    if not file_path.exists():
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="Uploaded file is no longer available on disk.",
        )

    # Load file safely
    try:
        df = await run_in_threadpool(_read_file, file_path)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to read file for detection: {str(exc)}",
        )

    # Run detection 
    detector = DetectorService()
    detected_schema = await run_in_threadpool(detector.detect_dataframe, df)

    metadata_json = import_session.metadata_json or {}

    import_session.metadata_json = {
        **metadata_json,
        "detected_schema": detected_schema,
    }

    # Advance state
    import_session.state = "DETECTED"

    try:
        await db.commit()
    except Exception as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to persist detected schema: {str(exc)}",
        )

    return {"columns": detected_schema}