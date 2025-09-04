# app/api/meta.py
import os, shutil, pandas as pd
from fastapi import APIRouter, UploadFile, File
from sqlalchemy import text
from ..db import get_engine

router = APIRouter()

def to_bool(v):
    if pd.isna(v): return None
    s = str(v).strip().lower()
    if s in {"1","true","t","si","sí","y","yes"}: return True
    if s in {"0","false","f","no","n"}: return False
    # fallback: números distintos de 0 -> True
    try:
        return bool(int(float(s)))
    except Exception:
        return s in {"x"}  # ajusta si manejas otra marca

@router.post("/upload")
def upload_meta(file: UploadFile = File(...)):
    path = os.path.join("uploads", f"meta_{file.filename}")
    os.makedirs("uploads", exist_ok=True)
    with open(path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    # Lee XLSX o CSV con autodetección de ; , \t
    if path.lower().endswith((".xls", ".xlsx")):
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path, sep=None, engine="python")

    df.columns = [c.strip().upper() for c in df.columns]
    if not {"CUENTA", "EFECTIVA"}.issubset(df.columns):
        return {"ok": False, "error": "META debe contener CUENTA y EFECTIVA"}

    # ✅ Normaliza a boolean
    df["EFECTIVA"] = df["EFECTIVA"].apply(to_bool)

    eng = get_engine()
    with eng.begin() as con:
        sql = text("""
            INSERT INTO meta_fraude (cuenta, efectiva)
            VALUES (:CUENTA, :EFECTIVA)
            ON CONFLICT (cuenta) DO UPDATE
              SET efectiva = EXCLUDED.efectiva,
                  updated_at = now()
        """)
        for r in df.to_dict("records"):
            con.execute(sql, r)

    return {"ok": True, "rows": int(len(df))}
