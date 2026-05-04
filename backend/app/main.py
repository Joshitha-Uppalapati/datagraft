
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import detect, export, mapping, upload, validation

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
    ],
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