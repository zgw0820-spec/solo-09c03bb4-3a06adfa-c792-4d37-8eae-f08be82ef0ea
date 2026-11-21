FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app ./app

RUN useradd --system --uid 10001 tiler \
    && mkdir -p /data \
    && chown tiler /data
USER tiler

# Runtime configuration (override in docker-compose.yml):
#   DATA_DIR             where uploads/staging/tars live
#   PPM_STAGING_BYTES    shared PPM staging quota in bytes
#   WORKERS              number of local converter processes
#   MAX_ACTIVE_BATCHES   batches allowed to pack concurrently
ENV DATA_DIR=/data \
    PPM_STAGING_BYTES=67108864 \
    WORKERS=2 \
    MAX_ACTIVE_BATCHES=2

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
