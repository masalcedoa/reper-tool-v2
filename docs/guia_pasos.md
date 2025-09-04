# Guía de Pasos (Patched)
1) Levantar stack: `docker compose up --build -d`
2) `/meta/upload` → admite XLSX o CSV con `,` o `;` (columnas: CUENTA, EFECTIVA)
3) `/ingest/upload` → XLSX/CSV (CUENTA, PERIODO, KWH)
4) `/jobs/{id}` → estado; `/jobs` → lista jobs
5) Power BI: vistas `vw_resultados_current`, `vw_alertas`, `vw_kpis`
