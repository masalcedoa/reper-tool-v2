CREATE TABLE IF NOT EXISTS model_registry(
  model_name TEXT NOT NULL, model_version TEXT NOT NULL, is_active BOOLEAN NOT NULL DEFAULT FALSE,
  threshold NUMERIC, metrics JSONB, notes TEXT, created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY(model_name,model_version)
);
CREATE OR REPLACE VIEW vw_active_models AS
SELECT model_name, model_version, threshold, metrics, notes, created_at
FROM model_registry WHERE is_active=TRUE;

CREATE OR REPLACE VIEW vw_cuenta_attrs AS
WITH last_row AS (
  SELECT DISTINCT ON (cuenta) cuenta, periodo, latitud, longitud, tipo_usuario, estrato, tipo_poblacion, fpas, trafo
  FROM stg_consumo ORDER BY cuenta, periodo DESC
) SELECT * FROM last_row;

CREATE OR REPLACE VIEW vw_resultados_current AS
SELECT r.* FROM resultados r JOIN vw_active_models a
  ON COALESCE(r.model_name,'hybrid_default')=a.model_name AND COALESCE(r.model_version,'1.0.0')=a.model_version;

CREATE OR REPLACE VIEW vw_alertas AS
SELECT r.*, c.trafo, c.estrato, c.tipo_usuario, c.tipo_poblacion
FROM vw_resultados_current r LEFT JOIN vw_cuenta_attrs c USING(cuenta);

CREATE OR REPLACE VIEW vw_kpis AS
SELECT date_trunc('month', r.created_at) AS mes, c.trafo, c.estrato,
       COUNT(*) AS total_cuentas, AVG(r.score_hibrido) AS avg_score,
       AVG((r.decision)::INT)::NUMERIC AS tasa_alerta
FROM vw_alertas r LEFT JOIN vw_cuenta_attrs c USING(cuenta)
GROUP BY 1,2,3 ORDER BY 1 DESC;
