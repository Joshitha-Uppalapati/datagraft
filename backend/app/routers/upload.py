import os
import uuid
import pandas as pd
from app.database import get_db
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models import ImportSession

router = APIRouter(prefix="/api")

ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx"}
MAX_UPLOAD_SIZE = 10 * 1024 * 1024  # 10MB


@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    filename = file.filename
    ext = os.path.splitext(filename)[1].lower()

    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=422, detail="Unsupported file type")

    file_id = str(uuid.uuid4())
    dir_path = f"/data/uploads/{file_id}"
    os.makedirs(dir_path, exist_ok=True)

    file_path = os.path.join(dir_path, "original.csv")

    size = 0
    chunk_size = 1024 * 1024

    with open(file_path, "wb") as f:
        while True:
            chunk = await file.read(chunk_size)
            if not chunk:
                break
            size += len(chunk)
            if size > MAX_UPLOAD_SIZE:
                f.close()
                os.remove(file_path)
                raise HTTPException(status_code=413, detail="File too large")
            f.write(chunk)

    try:
        if ext == ".csv":
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)

        row_count = len(df)
        col_count = len(df.columns)

    except Exception:
        os.remove(file_path)
        raise HTTPException(status_code=400, detail="Failed to read file")

    session = ImportSession(
        id=file_id,
        filename=filename,
        state="UPLOADED",
        stored_path=file_path,
        row_count=row_count,
        col_count=col_count,
        metadata_json={"source_extension": ext},
    )

    db.add(session)
    await db.commit()

    return {
        "file_id": file_id,
        "filename": filename,
        "row_count": row_count,
        "col_count": col_count,
    }


@router.get("/history")
async def get_import_history(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(ImportSession)
        .order_by(ImportSession.created_at.desc())
        .limit(20)
    )
    sessions = result.scalars().all()

    if not sessions:
        return []

    response = []

    for session in sessions:
        metadata = session.metadata_json or {}

        summary = metadata.get("validation_summary")
        if summary:
            validation_summary = {
                "total_rows": summary.get("total_rows"),
                "clean_rows": summary.get("clean_rows"),
                "error_rows": summary.get("error_rows"),
            }
        else:
            validation_summary = None

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