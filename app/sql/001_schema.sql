CREATE TABLE IF NOT EXISTS jobs(
  job_id UUID PRIMARY KEY, status TEXT NOT NULL DEFAULT 'queued', file_uri TEXT,
  created_at TIMESTAMPTZ DEFAULT now(), updated_at TIMESTAMPTZ DEFAULT now()
);
CREATE OR REPLACE FUNCTION trg_jobs_updated_at() RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS t_jobs_updated ON jobs;
CREATE TRIGGER t_jobs_updated BEFORE UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION trg_jobs_updated_at();

CREATE TABLE IF NOT EXISTS stg_consumo(
  cuenta TEXT NOT NULL, periodo DATE NOT NULL, kwh NUMERIC,
  latitud DOUBLE PRECISION, longitud DOUBLE PRECISION, tipo_usuario TEXT,
  estrato TEXT, tipo_poblacion TEXT, fpas TEXT, trafo TEXT, source_file TEXT,
  loaded_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY(cuenta,periodo)
);
CREATE TABLE IF NOT EXISTS meta_fraude(
  cuenta TEXT PRIMARY KEY, efectiva BOOLEAN, updated_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS features_curvas(
  cuenta TEXT PRIMARY KEY, prom_6 NUMERIC, std_12 NUMERIC, cv NUMERIC, benford_pval NUMERIC,
  computed_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS resultados(
  job_id UUID, cuenta TEXT,
  score_supervisado NUMERIC, score_curvas NUMERIC, score_hibrido NUMERIC,
  umbral_aplicado NUMERIC, decision BOOLEAN, model_name TEXT, model_version TEXT,
  created_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY(job_id,cuenta)
);
