FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN useradd --create-home --uid 10001 scanner \
    && mkdir -p /app/data /app/reports /app/logs \
    && chown -R scanner:scanner /app

WORKDIR /app

COPY requirements.txt pyproject.toml README.md ./
COPY config ./config
COPY src ./src
COPY scripts ./scripts

RUN pip install --upgrade pip \
    && pip install -e . \
    && chown -R scanner:scanner /app

USER scanner

EXPOSE 8501

# Default: Streamlit UI (scanner can be started from UI or a separate compose service)
CMD ["streamlit", "run", "src/polymarket_scanner/ui/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
