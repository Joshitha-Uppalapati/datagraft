import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import ImportSession
from app.services.mapper import MapperService

router = APIRouter(prefix="/api/map", tags=["mapping"])


class TargetSchemaField(BaseModel):
    name: str
    type: str
    required: bool
    variants: list[str] = Field(default_factory=list)


class MappingRequest(BaseModel):
    target_schema: list[TargetSchemaField]


class ConfirmedMappingItem(BaseModel):
    original: str
    canonical: str


class ConfirmMappingRequest(BaseModel):
    confirmed_mappings: list[ConfirmedMappingItem]


@router.post("/{file_id}")
async def generate_mapping_suggestions(
    file_id: uuid.UUID,
    payload: MappingRequest,
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

    metadata_json = import_session.metadata_json or {}
    detected_columns = metadata_json.get("detected_schema")

    if not detected_columns:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Detected schema not found. Run column detection first.",
        )

    mapper = MapperService()
    mappings = mapper.suggest_mappings(
        detected_columns=detected_columns,
        target_schema=[field.model_dump() for field in payload.target_schema],
    )

    updated_metadata = {
        **metadata_json,
        "mapping_suggestions": mappings,
        "target_schema": [field.model_dump() for field in payload.target_schema],
    }

    import_session.metadata_json = updated_metadata

    try:
        await db.commit()
    except Exception as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to persist mapping suggestions: {str(exc)}",
        )

    return {"mappings": mappings}


@router.post("/{file_id}/confirm")
async def confirm_mapping(
    file_id: uuid.UUID,
    payload: ConfirmMappingRequest,
    db: AsyncSession = Depends(get_db),
) -> dict[str, bool]:
    result = await db.execute(
        select(ImportSession).where(ImportSession.id == file_id)
    )
    import_session = result.scalar_one_or_none()

    if import_session is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Import session not found.",
        )

    metadata_json = import_session.metadata_json or {}

    updated_metadata = {
        **metadata_json,
        "confirmed_mappings": [item.model_dump() for item in payload.confirmed_mappings],
    }

    import_session.metadata_json = updated_metadata
    import_session.state = "MAPPING_CONFIRMED"

    try:
        await db.commit()
    except Exception as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to persist confirmed mappings: {str(exc)}",
        )

    return {"confirmed": True}