FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

COPY requirements.txt /tmp/requirements.txt

RUN pip install --upgrade pip \
    && pip install -r /tmp/requirements.txt APScheduler \
    && python -m playwright install --with-deps chromium

COPY . /app

RUN chmod +x /app/docker/entrypoint.sh \
    && DJANGO_DEBUG=0 DJANGO_ALLOWED_HOSTS="*" python manage.py collectstatic --noinput

ENTRYPOINT ["/app/docker/entrypoint.sh"]
CMD ["gunicorn", "oiat_portal.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "2", "--timeout", "180"]
