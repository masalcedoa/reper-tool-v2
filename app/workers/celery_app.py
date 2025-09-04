from celery import Celery
from ..config.settings import settings
celery_app=Celery('fraud_pipeline', broker=settings.REDIS_URL, backend=settings.REDIS_URL, include=['app.workers.tasks'])
