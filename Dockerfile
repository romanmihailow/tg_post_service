FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata procps \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD pgrep -f "python main.py" >/dev/null || exit 1

CMD ["python", "main.py"]
