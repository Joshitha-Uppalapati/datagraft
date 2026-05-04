
import os
import uuid
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import ImportSession

router = APIRouter(prefix="/api")

ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx"}
MAX_UPLOAD_SIZE_BYTES = 10 * 1024 * 1024
CHUNK_SIZE_BYTES = 1024 * 1024
UPLOAD_ROOT = Path("/data/uploads")


def _count_csv_shape(file_path: Path) -> tuple[int, int]:
    with file_path.open("r", encoding="utf-8-sig", newline="") as f:
        header = f.readline()
        if not header:
            return 0, 0

        col_count = len(header.rstrip("\n\r").split(","))
        row_count = sum(1 for line in f if line.strip())

    return row_count, col_count


def _count_excel_shape(file_path: Path) -> tuple[int, int]:
    df = pd.read_excel(file_path)
    return len(df), len(df.columns)


@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    filename = file.filename or ""
    ext = Path(filename).suffix.lower()

    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=422,
            detail="Unsupported file type. Upload a CSV or Excel file.",
        )

    file_id = uuid.uuid4()
    upload_dir = UPLOAD_ROOT / str(file_id)
    upload_dir.mkdir(parents=True, exist_ok=True)

    stored_path = upload_dir / f"original{ext}"
    bytes_read = 0

    try:
        with stored_path.open("wb") as out:
            while True:
                chunk = await file.read(CHUNK_SIZE_BYTES)
                if not chunk:
                    break

                bytes_read += len(chunk)
                if bytes_read > MAX_UPLOAD_SIZE_BYTES:
                    out.close()
                    stored_path.unlink(missing_ok=True)
                    raise HTTPException(
                        status_code=413,
                        detail="File is too large. Max upload size is 10MB.",
                    )

                out.write(chunk)

        if bytes_read == 0:
            stored_path.unlink(missing_ok=True)
            raise HTTPException(
                status_code=422,
                detail="Uploaded file is empty.",
            )

        if ext == ".csv":
            row_count, col_count = await run_in_threadpool(_count_csv_shape, stored_path)
        else:
            row_count, col_count = await run_in_threadpool(_count_excel_shape, stored_path)

        if row_count == 0 or col_count == 0:
            stored_path.unlink(missing_ok=True)
            raise HTTPException(
                status_code=422,
                detail="Uploaded file has no usable rows.",
            )

        import_session = ImportSession(
            id=file_id,
            filename=filename,
            state="UPLOADED",
            stored_path=str(stored_path),
            row_count=row_count,
            col_count=col_count,
            metadata_json={"source_extension": ext},
        )

        db.add(import_session)
        await db.commit()

        return {
            "file_id": str(file_id),
            "filename": filename,
            "row_count": row_count,
            "col_count": col_count,
        }

    except HTTPException:
        raise
    except Exception as exc:
        stored_path.unlink(missing_ok=True)
        raise HTTPException(
            status_code=400,
            detail=f"Could not process uploaded file: {str(exc)}",
        )


@router.get("/history")
async def get_import_history(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(ImportSession)
        .order_by(ImportSession.created_at.desc())
        .limit(20)
    )
    sessions = result.scalars().all()

    response = []

    for session in sessions:
        metadata = session.metadata_json or {}
        summary = metadata.get("validation_summary")

        validation_summary = None
        if summary:
            validation_summary = {
                "total_rows": summary.get("total_rows"),
                "clean_rows": summary.get("clean_rows"),
                "error_rows": summary.get("error_rows"),
            }

        response.append({
            "file_id": str(session.id),
            "filename": session.filename,
            "state": session.state,
            "row_count": session.row_count,
            "col_count": session.col_count,
            "created_at": session.created_at.isoformat(),
            "validation_summary": validation_summary,
        })

    return response