# app/workers/tasks.py
from __future__ import annotations

import os
import uuid
import numpy as np
import pandas as pd

from sqlalchemy import text
from .celery_app import celery_app
from ..db import get_engine
from ..utils.benford import benford_pval
from ..models.supervised import train_or_load, predict_proba

# ------------------------- Helpers comunes -------------------------

_ID_CANDIDATES = [
    "CUENTA", "NIS", "SUMINISTRO", "CODIGO SUMINISTRO", "CODIGO_SUMINISTRO",
    "ID", "CLIENTE", "NUMERO CUENTA", "NUMERO_CUENTA", "NIS_RAD", "NISRAD", "MEDIDOR"
]

_ATTR_CANON = {
    "LATITUD": ["LATITUD", "LAT", "LATITUDE"],
    "LONGITUD": ["LONGITUD", "LON", "LONG", "LONGITUDE"],
    "TIPO_USUARIO": ["TIPO USUARIO", "TIPO_USUARIO", "TIPO DE USUARIO", "SEGMENTO", "TIPOUSUARIO"],
    "ESTRATO": ["ESTRATO", "EST"],
    "TIPO_POBLACION": ["TIPO POBLACION", "TIPO_POBLACION", "TIPO DE POBLACION"],
    "FPAS": ["FPAS"],
    "TRAFO": ["TRAFO", "TRANSFORMADOR", "ID_TRAFO", "COD_TRAFO", "CODIGO TRAFO", "CODIGO_TRAFO"],
}

def _read_table(file_path: str) -> pd.DataFrame:
    """Lee XLSX o CSV autodetectando separador y codificación."""
    if file_path.lower().endswith((".xls", ".xlsx")):
        df = pd.read_excel(file_path)
    else:
        # Autodetecta sep y fallback de encoding
        for enc in ("utf-8", "latin-1"):
            try:
                df = pd.read_csv(file_path, sep=None, engine="python", encoding=enc)
                break
            except UnicodeDecodeError:
                continue
        else:
            df = pd.read_csv(file_path, sep=None, engine="python")
    # Normaliza encabezados a MAYÚSCULAS sin espacios extremos
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df

def _to_float(s: object) -> float | None:
    """Convierte string con punto/coma y separadores de miles a float."""
    if pd.isna(s):
        return None
    x = str(s).strip().replace(" ", "")
    # Heurística miles/decimales (ES/US)
    if x.count(",") > 0 and x.count(".") > 0:
        if x.rfind(",") > x.rfind("."):
            # decimal = coma → remove thousands dots
            x = x.replace(".", "").replace(",", ".")
        else:
            # decimal = punto → remove thousands commas
            x = x.replace(",", "")
    else:
        x = x.replace(",", ".")
    try:
        return float(x)
    except Exception:
        return None

def _parse_period_header(h: str) -> str | None:
    """Intenta parsear un nombre de columna que represente periodo (mensual) a 'YYYY-MM-01'."""
    import re
    s = str(h).strip().upper()
    if " " in s:
        s = s.split(" ")[0]
    s = s.lstrip("-# ")
    s = s.replace("\\", "/").replace("_", "-").replace(".", "-")
    s = re.sub(r"\s+", "-", s)

    m = re.match(r"^(\d{4})[-/ ]?(\d{1,2})(?:[-/ ]?(\d{1,2}))?$", s)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        if 1 <= mo <= 12:
            return f"{y:04d}-{mo:02d}-01"

    m = re.match(r"^(\d{4})(\d{2})(\d{2})?$", s)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        if 1 <= mo <= 12:
            return f"{y:04d}-{mo:02d}-01"

    m = re.search(r"(\d{4})(\d{2})", s)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        if 1 <= mo <= 12:
            return f"{y:04d}-{mo:02d}-01"
    return None

def _detect_id_col(df: pd.DataFrame) -> str:
    up = {c.upper(): c for c in df.columns}
    for cand in _ID_CANDIDATES:
        if cand.upper() in up:
            return up[cand.upper()]
    return df.columns[0]

def _detect_attributes(df: pd.DataFrame) -> dict[str, str]:
    up = {c.upper(): c for c in df.columns}
    out: dict[str, str] = {}
    for canon, options in _ATTR_CANON.items():
        for opt in options:
            if opt.upper() in up:
                out[canon] = up[opt.upper()]
                break
    return out

def _longify_if_wide(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte formato ancho (meses por columnas) a largo (CUENTA, PERIODO, KWH)."""
    id_col = _detect_id_col(df)
    attrib_cols = _detect_attributes(df)

    period_cols, period_map = [], {}
    for c in df.columns:
        if c == id_col or c in attrib_cols.values():
            continue
        pp = _parse_period_header(c)
        if pp:
            period_cols.append(c)
            period_map[c] = pp

    if not period_cols:
        for c in df.columns[7:]:
            pp = _parse_period_header(c)
            if pp:
                period_cols.append(c)
                period_map[c] = pp

    if not period_cols:
        return df

    keep_attrs = list(attrib_cols.values())
    m = df[[id_col] + keep_attrs + period_cols].melt(
        id_vars=[id_col] + keep_attrs,
        value_vars=period_cols,
        var_name="PERIODO_RAW",
        value_name="KWH"
    )
    m["PERIODO"] = m["PERIODO_RAW"].map(period_map)
    m["KWH"] = m["KWH"].apply(_to_float)
    m["CUENTA"] = m[id_col].astype(str).str.strip()
    m["PERIODO"] = pd.to_datetime(m["PERIODO"], errors="coerce").dt.date
    m = m.dropna(subset=["PERIODO", "KWH"])

    out = m[["CUENTA", "PERIODO", "KWH"]].copy()
    out["LATITUD"]  = m[attrib_cols["LATITUD"]].apply(_to_float) if "LATITUD" in attrib_cols else np.nan
    out["LONGITUD"] = m[attrib_cols["LONGITUD"]].apply(_to_float) if "LONGITUD" in attrib_cols else np.nan
    for key in ["TIPO_USUARIO", "ESTRATO", "TIPO_POBLACION", "FPAS", "TRAFO"]:
        out[key] = m[attrib_cols[key]].astype(str).str.strip() if key in attrib_cols else np.nan

    agg = {
        "KWH": "sum",
        "LATITUD": "first", "LONGITUD": "first", "TIPO_USUARIO": "first", "ESTRATO": "first",
        "TIPO_POBLACION": "first", "FPAS": "first", "TRAFO": "first",
    }
    out2 = out.groupby(["CUENTA", "PERIODO"], as_index=False).agg(agg)
    return out2

# ------------------------- Tareas del pipeline -------------------------

@celery_app.task
def ingest_consumo(job_id: str, file_path: str):
    """Ingesta a stg_consumo; detecta ancho→largo; upsert y lanza MCURVAS."""
    eng = get_engine()
    df = _read_table(file_path)

    req = {"CUENTA", "PERIODO", "KWH"}
    if not req.issubset(df.columns):
        df = _longify_if_wide(df)
        if not req.issubset(df.columns):
            raise ValueError("El archivo no contiene columnas requeridas CUENTA, PERIODO, KWH (ni formato ancho detectable)." )

    df["PERIODO"] = pd.to_datetime(df["PERIODO"], errors="coerce").dt.date
    df["KWH"] = pd.to_numeric(df["KWH"], errors="coerce")
    df = df.dropna(subset=["CUENTA", "PERIODO", "KWH"])

    with eng.begin() as con:
        con.execute(text("UPDATE jobs SET status='ingesting' WHERE job_id=:j"), {"j": uuid.UUID(job_id)})
        ins = text("""
        INSERT INTO stg_consumo
          (cuenta, periodo, kwh, latitud, longitud, tipo_usuario, estrato, tipo_poblacion, fpas, trafo, source_file)
        VALUES
          (:CUENTA, :PERIODO, :KWH, :LATITUD, :LONGITUD, :TIPO_USUARIO, :ESTRATO, :TIPO_POBLACION, :FPAS, :TRAFO, :SOURCE_FILE)
        ON CONFLICT (cuenta,periodo) DO UPDATE
          SET kwh=EXCLUDED.kwh,
              latitud=COALESCE(EXCLUDED.latitud, stg_consumo.latitud),
              longitud=COALESCE(EXCLUDED.longitud, stg_consumo.longitud),
              tipo_usuario=COALESCE(EXCLUDED.tipo_usuario, stg_consumo.tipo_usuario),
              estrato=COALESCE(EXCLUDED.estrato, stg_consumo.estrato),
              tipo_poblacion=COALESCE(EXCLUDED.tipo_poblacion, stg_consumo.tipo_poblacion),
              fpas=COALESCE(EXCLUDED.fpas, stg_consumo.fpas),
              trafo=COALESCE(EXCLUDED.trafo, stg_consumo.trafo)
        """)
        for opt in ["LATITUD","LONGITUD","TIPO_USUARIO","ESTRATO","TIPO_POBLACION","FPAS","TRAFO"]:
            if opt not in df.columns:
                df[opt] = np.nan
        for rec in df.to_dict("records"):
            rec["SOURCE_FILE"] = os.path.basename(file_path)
            con.execute(ins, rec)

    mcurvas_prepare.delay(job_id)
    return {"rows": int(len(df))}

@celery_app.task
def mcurvas_prepare(job_id: str):
    """Calcula features de curvas y las guarda en features_curvas."""
    eng = get_engine()
    with eng.begin() as con:
        con.execute(text("UPDATE jobs SET status='mcurvas' WHERE job_id=:j"), {"j": uuid.UUID(job_id)})
        df = pd.read_sql(text("SELECT cuenta, periodo, kwh FROM stg_consumo"), con)

    if df.empty:
        msupervisado_score.delay(job_id)
        return {"features": 0}

    df = df.sort_values(["CUENTA", "PERIODO"])
    last12 = df.groupby("CUENTA").tail(12)

    piv = last12.pivot_table(index="CUENTA", columns="PERIODO", values="KWH", aggfunc="last").sort_index(axis=1)
    piv = piv.fillna(0.0)
    arr = piv.to_numpy()

    if arr.shape[1] >= 6:
        prom_6 = arr[:, -6:].mean(axis=1)
        sample_for_benford = [row[-6:] for row in arr]
    else:
        prom_6 = arr.mean(axis=1)
        sample_for_benford = [row for row in arr]

    std_12 = arr.std(axis=1, ddof=0)
    cv = std_12 / (prom_6 + 1e-6)
    ben = [benford_pval(sample) for sample in sample_for_benford]

    out = pd.DataFrame({
        "cuenta": piv.index,
        "prom_6": prom_6,
        "std_12": std_12,
        "cv": cv,
        "benford_pval": ben
    })

    with eng.begin() as con:
        up = text("""
        INSERT INTO features_curvas (cuenta, prom_6, std_12, cv, benford_pval)
        VALUES (:cuenta, :prom_6, :std_12, :cv, :benford_pval)
        ON CONFLICT (cuenta) DO UPDATE
           SET prom_6=EXCLUDED.prom_6,
               std_12=EXCLUDED.std_12,
               cv=EXCLUDED.cv,
               benford_pval=EXCLUDED.benford_pval,
               computed_at=now()
        """)
        for _, r in out.iterrows():
            con.execute(up, r.to_dict())

    msupervisado_score.delay(job_id)
    return {"features": int(len(out))}

@celery_app.task
def msupervisado_score(job_id: str):
    """Entrena/usa modelo supervisado y envía registros a hibridación."""
    eng = get_engine()
    with eng.begin() as con:
        con.execute(text("UPDATE jobs SET status='msupervisado' WHERE job_id=:j"), {"j": uuid.UUID(job_id)})
        train_df = pd.read_sql(text("""
            SELECT f.cuenta, f.prom_6, f.std_12, f.cv, f.benford_pval, m.efectiva::int AS y
            FROM features_curvas f
            JOIN meta_fraude m USING(cuenta)
        """), con)
        X_all = pd.read_sql(text("""
            SELECT f.cuenta, f.prom_6, f.std_12, f.cv, f.benford_pval
            FROM features_curvas f
        """), con)

    if X_all.empty:
        hibridacion.delay(job_id, [])
        return {"scored": 0}

    if len(train_df) < 30 or train_df["y"].nunique() < 2:
        X = X_all[["prom_6", "std_12", "cv", "benford_pval"]].fillna(0.0).to_numpy()
        score_sup = np.full((X.shape[0],), 0.5)
    else:
        X = train_df[["prom_6", "std_12", "cv", "benford_pval"]].fillna(0.0).to_numpy()
        y = train_df["y"].to_numpy()
        model = train_or_load(X, y)
        Xp = X_all[["prom_6", "std_12", "cv", "benford_pval"]].fillna(0.0).to_numpy()
        score_sup = predict_proba(model, Xp)

    X_all["score_supervisado"] = score_sup
    recs = X_all[["cuenta", "score_supervisado"]].to_dict("records")
    hibridacion.delay(job_id, recs)
    return {"scored": int(len(X_all))}

@celery_app.task
def hibridacion(job_id: str, supervised_records: list[dict]):
    """Aplica umbral activo y persiste en resultados."""
    eng = get_engine()
    with eng.begin() as con:
        con.execute(text("UPDATE jobs SET status='hibridacion' WHERE job_id=:j"), {"j": uuid.UUID(job_id)})
        row = con.execute(text("SELECT model_name, model_version, threshold FROM vw_active_models LIMIT 1")).mappings().first()

    model_name = row["model_name"] if row else "hybrid_default"
    model_version = row["model_version"] if row else "1.0.0"
    thr = float(row["threshold"]) if (row and row["threshold"] is not None) else 0.60

    out = []
    for r in supervised_records:
        cuenta = r["cuenta"]
        s_sup = float(r["score_supervisado"])
        s_cur = None
        s_h = s_sup
        dec = s_h >= thr
        out.append((cuenta, s_sup, s_cur, s_h, thr, dec, model_name, model_version))

    with eng.begin() as con:
        ins = text("""
        INSERT INTO resultados
          (job_id, cuenta, score_supervisado, score_curvas, score_hibrido, umbral_aplicado, decision, model_name, model_version)
        VALUES
          (:job, :cuenta, :ss, :sc, :sh, :thr, :dec, :mname, :mver)
        ON CONFLICT (job_id, cuenta) DO UPDATE
           SET score_supervisado=EXCLUDED.score_supervisado,
               score_curvas=EXCLUDED.score_curvas,
               score_hibrido=EXCLUDED.score_hibrido,
               umbral_aplicado=EXCLUDED.umbral_aplicado,
               decision=EXCLUDED.decision,
               model_name=EXCLUDED.model_name,
               model_version=EXCLUDED.model_version
        """)
        for (cuenta, s_sup, s_cur, s_h, t, dec, mname, mver) in out:
            con.execute(ins, {
                "job": uuid.UUID(job_id),
                "cuenta": cuenta,
                "ss": s_sup,
                "sc": s_cur,
                "sh": s_h,
                "thr": t,
                "dec": bool(dec),
                "mname": mname,
                "mver": mver
            })

    predict_publish.delay(job_id)
    return {"hybrid_rows": int(len(out))}

@celery_app.task
def predict_publish(job_id: str):
    eng = get_engine()
    with eng.begin() as con:
        con.execute(text("UPDATE jobs SET status='done' WHERE job_id=:j"), {"j": uuid.UUID(job_id)})
    return {"status": "done"}
