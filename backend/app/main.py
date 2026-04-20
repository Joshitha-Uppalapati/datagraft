from fastapi import FastAPI

from app.config import settings
from app.routers.detect import router as detect_router
from app.routers.mapping import router as mapping_router
from app.routers.upload import router as upload_router

app = FastAPI(title=settings.app_name)

app.include_router(upload_router)
app.include_router(detect_router)
app.include_router(mapping_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}