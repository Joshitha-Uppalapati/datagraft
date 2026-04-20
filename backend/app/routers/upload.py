import io
import uuid
from pathlib import Path
from zipfile import BadZipFile

import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.models import ImportSession

router = APIRouter(prefix="/api", tags=["upload"])

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}


def _validate_extension(filename: str) -> str:
    suffix = Path(filename).suffix.lower()
    if suffix not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Only .csv, .xlsx, and .xls files are allowed.",
        )
    return suffix


def _read_csv(file_bytes: bytes) -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(file_bytes))


def _read_excel(file_bytes: bytes, suffix: str) -> pd.DataFrame:
    engine = "openpyxl" if suffix == ".xlsx" else "xlrd"
    return pd.read_excel(io.BytesIO(file_bytes), engine=engine)


def _write_csv(df: pd.DataFrame, output_path: Path) -> None:
    df.to_csv(output_path, index=False)


@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
) -> dict:
    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Filename is required.",
        )

    suffix = _validate_extension(file.filename)

    try:
        file_bytes = await file.read()
    finally:
        await file.close()

    if not file_bytes:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="File is empty.",
        )

    if len(file_bytes) > settings.max_upload_size_bytes:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="File exceeds 10MB limit.",
        )

    try:
        if suffix == ".csv":
            df = await run_in_threadpool(_read_csv, file_bytes)
        else:
            df = await run_in_threadpool(_read_excel, file_bytes, suffix)
    except pd.errors.EmptyDataError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="File has no rows.",
        )
    except (ValueError, BadZipFile, OSError):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Corrupt or unreadable Excel file.",
        )
    except Exception as exc:
        # Keep CSV parse failures explicit and user-facing
        if suffix == ".csv":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unable to parse CSV file: {str(exc)}",
            )
        raise

    if df.empty:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="File has no rows.",
        )

    session_id = uuid.uuid4()
    session_dir = Path(settings.upload_base_dir) / str(session_id)
    session_dir.mkdir(parents=True, exist_ok=True)

    stored_path = session_dir / "original.csv"

    try:
        await run_in_threadpool(_write_csv, df, stored_path)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to store file on disk: {str(exc)}",
        )

    import_session = ImportSession(
        id=session_id,
        filename=file.filename,
        stored_path=str(stored_path),
        state="UPLOADED",
        row_count=int(df.shape[0]),
        col_count=int(df.shape[1]),
        metadata_json={
            "source_extension": suffix,
        },
    )

    db.add(import_session)

    try:
        await db.commit()
    except Exception as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create import session: {str(exc)}",
        )

    return {
        "file_id": str(session_id),
        "filename": file.filename,
        "row_count": int(df.shape[0]),
        "col_count": int(df.shape[1]),
    }