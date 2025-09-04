import uuid
from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from ..db import get_engine
router=APIRouter()
@router.get('/jobs/{job_id}')
def job_status(job_id:str):
    try: jid=uuid.UUID(job_id)
    except Exception: raise HTTPException(status_code=400, detail='invalid job_id')
    eng=get_engine()
    with eng.connect() as con:
        r=con.execute(text("SELECT job_id::text,status,file_uri,created_at,updated_at FROM jobs WHERE job_id=:j"), {'j':jid}).mappings().first()
        if not r: raise HTTPException(status_code=404, detail='job not found')
        return dict(r)
@router.get('/jobs')
def list_jobs(limit:int=50):
    eng=get_engine()
    with eng.connect() as con:
        rows=con.execute(text("""SELECT job_id::text,status,file_uri,created_at,updated_at
                                FROM jobs ORDER BY created_at DESC LIMIT :l"""), {'l':limit}).mappings().all()
        return [dict(r) for r in rows]
