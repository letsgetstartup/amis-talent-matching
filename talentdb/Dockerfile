########## TalentDB API Dockerfile (Mongo-only) ##########
# Multi-stage for smaller final image.

FROM python:3.13-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app

FROM base AS builder
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
COPY requirements.txt ./
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM base AS runtime
ENV PATH="/install/bin:$PATH"
COPY --from=builder /install /usr/local
COPY . .
RUN chmod +x run_api.sh
EXPOSE 8000
ENV MONGO_URI=mongodb://mongo:27017 \
    DB_NAME=talent_match \
    API_KEY=changeme
# Basic healthcheck hitting readiness
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD wget -q -O - http://127.0.0.1:8000/ready || exit 1
CMD ["./run_api.sh"]
