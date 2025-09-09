web: uvicorn app:app --host 0.0.0.0 --port $PORT
worker: celery -A tasks.celery worker --loglevel=info