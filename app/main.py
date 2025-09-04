from fastapi import FastAPI
from .api import health, ingest, meta, jobs
from .db import init_db
def create_app():
    init_db()
    app=FastAPI(title='Fraud Automation API (Patched)')
    app.include_router(health.router, tags=['health'])
    app.include_router(ingest.router, prefix='/ingest', tags=['ingest'])
    app.include_router(meta.router, prefix='/meta', tags=['meta'])
    app.include_router(jobs.router, tags=['jobs'])
    return app
app=create_app()
