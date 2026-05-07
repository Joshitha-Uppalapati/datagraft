import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.utils import read_file
from app.database import get_db
from app.models import ImportSession
from app.services.validator import ValidatorService

router = APIRouter(prefix="/api", tags=["validation"])

@router.get("/validate/{file_id}")
async def validate_file(
    file_id: uuid.UUID,
    error_limit: int = Query(default=100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    result = await db.execute(
        select(ImportSession).where(ImportSession.id == file_id)
    )
    import_session = result.scalar_one_or_none()

    if import_session is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Import session not found.",
        )

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

    try:
        df = await run_in_threadpool(read_file, str(file_path))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to read file for validation: {str(exc)}",
        )

    validator = ValidatorService()
    validation_result = await run_in_threadpool(
        validator.validate_dataframe,
        df,
        confirmed_mappings,
        target_schema,
        error_limit,
    )

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