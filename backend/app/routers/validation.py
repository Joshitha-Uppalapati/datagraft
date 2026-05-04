import uuid
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import ImportSession
from app.services.validator import ValidatorService

router = APIRouter(prefix="/api", tags=["validation"])


def _read_file(file_path: Path) -> pd.DataFrame:
    if file_path.suffix.lower() == ".csv":
        return pd.read_csv(file_path)
    return pd.read_excel(file_path)


@router.get("/validate/{file_id}")
async def validate_file(
    file_id: uuid.UUID,
    error_limit: int = Query(default=100, ge=1, le=1000),
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
    if import_session.state != "MAPPING_CONFIRMED":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid state '{import_session.state}'. Validation requires 'MAPPING_CONFIRMED'.",
        )

    metadata_json = import_session.metadata_json or {}
    confirmed_mappings = metadata_json.get("confirmed_mappings")
    target_schema = metadata_json.get("target_schema")

    if not confirmed_mappings:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Confirmed mappings missing. Cannot validate.",
        )

    if not target_schema:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Target schema missing. Cannot validate.",
        )

    file_path = Path(import_session.stored_path)

    if not file_path.exists():
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="Uploaded file is no longer available on disk.",
        )

    # Load file 
    try:
        df = await run_in_threadpool(_read_file, file_path)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to read file for validation: {str(exc)}",
        )

    # Run validation
    validator = ValidatorService()
    validation_result = await run_in_threadpool(
        validator.validate_dataframe,
        df,
        confirmed_mappings,
        target_schema,
        error_limit,
    )

    #  Persist results
    import_session.metadata_json = {
        **metadata_json,
        "validation_summary": {
            "total_rows": validation_result["total_rows"],
            "clean_rows": validation_result["clean_rows"],
            "error_rows": validation_result["error_rows"],
            "errors_truncated": validation_result["errors_truncated"],
        },
        "validation_errors": validation_result["errors"],
        "all_error_indices": validation_result["all_error_indices"],
    }

    import_session.state = "VALIDATED"

    try:
        await db.commit()
    except Exception as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to persist validation results: {str(exc)}",
        )

    return validation_result