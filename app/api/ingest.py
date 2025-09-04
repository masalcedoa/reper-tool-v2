# app/api/ingest.py
import os, uuid, shutil
from fastapi import APIRouter, UploadFile, File
from sqlalchemy import text
from ..db import get_engine
from ..workers.tasks import ingest_consumo
from ..utils.s3 import presign_put, object_uri

router = APIRouter()

@router.post('/upload')
def local_upload(file: UploadFile = File(...)):
    out_dir = 'uploads'
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, file.filename)
    with open(out_path, 'wb') as f:
        shutil.copyfileobj(file.file, f)

    eng = get_engine()
    jid = uuid.uuid4()
    # ⬇️ USAR con.execute(text(...), params) (no exec_driver_sql con :param)
    with eng.begin() as con:
        con.execute(
            text("INSERT INTO jobs(job_id,status,file_uri) VALUES (:j,'queued',:u)"),
            {"j": jid, "u": out_path}
        )
    # Encolar el trabajo
    ingest_consumo.delay(str(jid), out_path)
    return {"job_id": str(jid), "file_uri": out_path}
