# AdPulse Python services image (generator, auditor, API, dashboard, init jobs).
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

# Optional extra CA certificates (only needed behind a TLS-inspecting proxy;
# on a normal computer docker/certs/ is empty and this step does nothing).
COPY docker/certs/ /tmp/extra-certs/
RUN if ls /tmp/extra-certs/*.crt >/dev/null 2>&1; then \
      cp /tmp/extra-certs/*.crt /usr/local/share/ca-certificates/ && update-ca-certificates; \
    fi

WORKDIR /app
COPY requirements/app.txt requirements/app.txt
RUN PIP_CERT=/etc/ssl/certs/ca-certificates.crt pip install --no-cache-dir -r requirements/app.txt

COPY src/ src/
COPY .streamlit/ .streamlit/

# Run as a normal user, not root (security best practice).
RUN useradd --create-home --uid 10001 adpulse && chown -R adpulse /app
USER adpulse
