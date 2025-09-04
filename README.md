# Automatización Fraude Energía — Repositorio Parcheado

### Cambios clave
- `openpyxl` agregado (lee `.xlsx`).
- Lectura **CSV con `sep=None`** (autodetecta `,` / `;` / `\t`).
- Arreglo SQLAlchemy 2.x: se usa `con.execute(text(...), params)` cuando se arma `text()`.
- MinIO `:latest` para evitar tags obsoletos.
- Endpoint `GET /jobs` para listar jobs recientes.

### Flujo
1) **MCURVAS** → features básicas.
2) **MSUPERVISADO** → score supervisado.
3) **hibridacion** → aplica umbral activo.
4) **predict** → publica en `resultados` (vistas para BI).

### Arranque
```bash
cp .env.example .env
docker compose up --build -d
# Docs -> http://localhost:8000/docs
```

### Endpoints
- `POST /meta/upload` → sube META (XLSX/CSV con ; o ,) → tabla `meta_fraude`.
- `POST /ingest/upload` → sube Consumos (XLSX/CSV) → dispara pipeline.
- `GET /jobs/{job_id}` → estado del job.
- `GET /jobs` → lista últimos jobs.

### Postman (opcional)
En `/postman/` hay colección y ambiente listos.
