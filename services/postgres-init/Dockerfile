FROM python:3.11-slim

WORKDIR /app

# Copy and install shared library
COPY libs/common /app/libs/common
RUN pip install --no-cache-dir -e /app/libs/common

# Copy and install K8s Observability library
COPY libs/k8s-observability /app/libs/k8s-observability
RUN pip install --no-cache-dir -e /app/libs/k8s-observability

# Copy service code
COPY services/postgres-init /app/services/postgres-init

# Create non-root user for security
RUN useradd -m -u 1000 opendt && \
    chown -R opendt:opendt /app

USER opendt

# Set working directory to service for proper module resolution
WORKDIR /app/services/postgres-init

CMD ["python", "-m", "postgres_init.main"]
