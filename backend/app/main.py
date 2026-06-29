from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import detect, export, mapping, upload, validation

app = FastAPI(title=settings.app_name)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(upload.router)
app.include_router(detect.router)
app.include_router(mapping.router)
app.include_router(validation.router)
app.include_router(export.router)


@app.get("/health")
async def health_check():
    return {"status": "ok"}