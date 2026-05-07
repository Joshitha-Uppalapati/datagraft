from pathlib import Path

import pandas as pd
from fastapi import HTTPException, status


def read_file(file_path: str) -> pd.DataFrame:
    path = Path(file_path)

    if not path.exists():
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="Uploaded file is no longer available on disk.",
        )

    try:
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path)
        return pd.read_excel(path)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to read file: {str(exc)}",
        )