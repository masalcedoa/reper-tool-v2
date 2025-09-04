# Despliegue en Droplet (DigitalOcean)

## 1) Crear droplet y preparar servidor
- Ubuntu 22.04 LTS, 2 vCPU / 4 GB recomendado.
- Agrega tu SSH Key al crear el droplet.
- Conéctate: `ssh root@<IP>`

Instala Docker/Compose (script incluido):
```bash
chmod +x bootstrap.sh && ./bootstrap.sh
# Reinicia sesión SSH para aplicar grupo docker.
```

## 2) Subir el proyecto
- **VS Code Remote SSH**: conecta al droplet y abre `/opt/fraude-app`.
- O **git clone** en `/opt/fraude-app`.

Copia estos archivos (`docker-compose.yml`, `.env.example`, `Caddyfile`, `bootstrap.sh`, `deploy.sh`, `README`) al directorio del proyecto.

## 3) Variables
```bash
cp .env.example .env
# Edita DB_PASSWORD y otros.
```

## 4) (Opcional) Dominio + TLS
- Apunta tu dominio al IP del droplet (A record).
- Cambia `tu-dominio.com` en `Caddyfile`.

## 5) Levantar
```bash
chmod +x deploy.sh
./deploy.sh --build
```

## 6) Probar
```bash
curl http://<IP>:8000/health
# o https://tu-dominio.com/health si activaste Caddy
```

## 7) Logs / mantenimiento
```bash
docker compose ps
docker compose logs -f api
docker compose logs -f worker
docker compose restart api worker
```

## 8) Seguridad
- `ufw`:
```bash
sudo ufw allow OpenSSH
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable
```
- Puertos 5432/6379 expuestos solo en localhost ya por compose.

## 9) Backups
```bash
docker compose exec -T postgres pg_dump -U ${DB_USER} ${DB_NAME} > backup.sql
```

## 10) Actualizar con VS Code
- Edita archivos en `/opt/fraude-app` (Remote SSH).
- Reinicia:
```bash
docker compose restart api worker
# Si cambiaste Dockerfiles:
./deploy.sh --build
```
