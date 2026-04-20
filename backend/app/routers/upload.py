
import os
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import ImportSession

router = APIRouter(prefix="/api", tags=["upload"])

UPLOAD_DIR = Path("/tmp/datagraft_uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

MAX_UPLOAD_SIZE_BYTES = 10 * 1024 * 1024  # 10MB
CHUNK_SIZE = 1024 * 1024  # 1MB


@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    file_id = uuid.uuid4()
    file_path = UPLOAD_DIR / f"{file_id}_{file.filename}"

    total_bytes = 0

    try:
        with open(file_path, "wb") as f:
            while True:
                chunk = await file.read(CHUNK_SIZE)
                if not chunk:
                    break

                total_bytes += len(chunk)

                # We enforce the limit during streaming to avoid loading large files into memory.
                if total_bytes > MAX_UPLOAD_SIZE_BYTES:
                    f.close()
                    os.remove(file_path)
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail="File exceeds maximum allowed size.",
                    )

                f.write(chunk)

    except HTTPException:
        raise
    except Exception as e:
        if file_path.exists():
            os.remove(file_path)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload file: {str(e)}",
        )

    import_session = ImportSession(
        id=file_id,
        filename=file.filename,
        stored_path=str(file_path),
        state="UPLOADED",
        metadata_json={},
    )

    db.add(import_session)

    try:
        await db.commit()
    except Exception as e:
        await db.rollback()
        os.remove(file_path)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to persist upload metadata: {str(e)}",
        )

    return {
        "file_id": str(file_id),
        "filename": file.filename,
        "size_bytes": total_bytes,
    }