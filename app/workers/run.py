import sys, uuid, numpy as np, pandas as pd
from sqlalchemy import text
from app.db import get_engine

# Intentamos usar las utilidades del repo; si no, haremos baseline.
try:
    from app.models.supervised import train_or_load, predict_proba
    HAVE_MODEL = True
except Exception:
    HAVE_MODEL = False

from app.utils.benford import benford_pval

def compute_features(eng):
    df = pd.read_sql(text("SELECT cuenta, periodo, kwh FROM stg_consumo"), eng)
    if df.empty:
        print("[MCURVAS] stg_consumo está vacío.")
        return 0
    df = df.sort_values(["cuenta","periodo"])
    last12 = df.groupby("cuenta").tail(12)
    piv = (last12
           .pivot_table(index="cuenta", columns="periodo", values="kwh", aggfunc="last")
           .sort_index(axis=1)
           .fillna(0.0))
    arr = piv.to_numpy()

    if arr.shape[1] >= 6:
        prom_6 = arr[:,-6:].mean(axis=1)
        samples = [row[-6:] for row in arr]
    else:
        prom_6 = arr.mean(axis=1)
        samples = [row for row in arr]

    std_12 = arr.std(axis=1, ddof=0)
    cv = std_12 / (prom_6 + 1e-6)
    ben = [benford_pval(s) for s in samples]

    feats = pd.DataFrame({
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
        for _, r in feats.iterrows():
            con.execute(up, r.to_dict())
    print(f"[MCURVAS] filas escritas en features_curvas: {len(feats)}")
    return len(feats)

def supervised_and_publish(eng, job_id):
    # Entrenamiento (si hay META suficiente) y scoring
    train_df = pd.read_sql(text("""
        SELECT f.prom_6,f.std_12,f.cv,f.benford_pval, m.efectiva::int y
        FROM features_curvas f JOIN meta_fraude m USING(cuenta)
    """), eng)
    X_all = pd.read_sql(text("""
        SELECT f.cuenta, f.prom_6,f.std_12,f.cv,f.benford_pval
        FROM features_curvas f
    """), eng)

    if X_all.empty:
        print("[SUP/HIBRID] No hay features para scorear.")
        return 0

    if len(train_df) >= 30 and train_df["y"].nunique() >= 2 and HAVE_MODEL:
        try:
            X = train_df[["prom_6","std_12","cv","benford_pval"]].fillna(0.0).to_numpy()
            y = train_df["y"].to_numpy()
            model = train_or_load(X, y)
            Xp = X_all[["prom_6","std_12","cv","benford_pval"]].fillna(0.0).to_numpy()
            scores = predict_proba(model, Xp)
            print(f"[SUP] Modelo entrenado. Scored={len(scores)}")
        except Exception as e:
            print(f"[SUP] Falla al entrenar/scorear ({e}). Uso baseline 0.5")
            scores = np.full((len(X_all),), 0.5)
    else:
        print("[SUP] Insuficiente META o sin modelo. Uso baseline 0.5")
        scores = np.full((len(X_all),), 0.5)

    X_all["score_supervisado"] = scores
    recs = X_all[["cuenta","score_supervisado"]].to_dict("records")

    # Umbral activo
    with eng.begin() as con:
        row = con.execute(text("SELECT model_name, model_version, threshold FROM vw_active_models LIMIT 1")).mappings().first()
    thr = float(row["threshold"]) if row and row["threshold"] is not None else 0.60
    mname = row["model_name"] if row else "hybrid_default"
    mver  = row["model_version"] if row else "1.0.0"

    # Publicación en resultados
    written = 0
    with eng.begin() as con:
        ins = text("""
        INSERT INTO resultados (job_id, cuenta, score_supervisado, score_curvas, score_hibrido, umbral_aplicado, decision, model_name, model_version)
        VALUES (:job,:cuenta,:ss,:sc,:sh,:thr,:dec,:mname,:mver)
        ON CONFLICT (job_id, cuenta) DO UPDATE
           SET score_supervisado=EXCLUDED.score_supervisado,
               score_curvas=EXCLUDED.score_curvas,
               score_hibrido=EXCLUDED.score_hibrido,
               umbral_aplicado=EXCLUDED.umbral_aplicado,
               decision=EXCLUDED.decision,
               model_name=EXCLUDED.model_name,
               model_version=EXCLUDED.model_version
        """)
        for r in recs:
            sh = float(r["score_supervisado"])
            dec = sh >= thr
            con.execute(ins, {
                "job": uuid.UUID(job_id),
                "cuenta": r["cuenta"],
                "ss": sh,
                "sc": None,
                "sh": sh,
                "thr": thr,
                "dec": bool(dec),
                "mname": mname,
                "mver": mver
            })
            written += 1

    # Marcar job como done
    with eng.begin() as con:
        con.execute(text("UPDATE jobs SET status='done' WHERE job_id=:j"), {"j": uuid.UUID(job_id)})

    print(f"[HIBRID/PUBLISH] filas escritas en resultados: {written}")
    return written

def main():
    if len(sys.argv) < 2:
        print("Uso: python app/workers/manual_run.py <job_id>")
        sys.exit(1)
    job_id = sys.argv[1]
    eng = get_engine()

    # Estados informativos (no obligatorio)
    with eng.begin() as con:
        con.execute(text("UPDATE jobs SET status='mcurvas' WHERE job_id=:j"), {"j": uuid.UUID(job_id)})
    f = compute_features(eng)

    with eng.begin() as con:
        con.execute(text("UPDATE jobs SET status='msupervisado' WHERE job_id=:j"), {"j": uuid.UUID(job_id)})
    r = supervised_and_publish(eng, job_id)

    print(f"[RESUMEN] features={f}, resultados={r}, job_id={job_id}")

if __name__ == "__main__":
    main()

