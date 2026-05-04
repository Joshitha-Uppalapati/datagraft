from fastapi import FastAPI

from app.routers import upload, detect, mapping, validation, export

app = FastAPI()

app.include_router(upload.router)
app.include_router(detect.router)
app.include_router(mapping.router)
app.include_router(validation.router)
app.include_router(export.router)

@app.get("/health")
async def health_check():
    return {"status": "ok"}