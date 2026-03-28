FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy and install dependencies from root pyproject.toml
COPY pyproject.toml /app/pyproject.toml
RUN pip install --no-cache-dir -e /app
RUN pip install --no-cache-dir -e ".[k8s-decision-maker]"

# Copy and install shared library
COPY libs/common /app/libs/common
RUN pip install --no-cache-dir -e /app/libs/common

# Copy and install K8s Observability library
COPY libs/k8s-observability /app/libs/k8s-observability
RUN pip install --no-cache-dir -e /app/libs/k8s-observability

# Copy service code
COPY services/k8s-decision-maker /app/services/k8s-decision-maker

# Create non-root user for security
RUN useradd -m -u 1000 opendt && \
    chown -R opendt:opendt /app

USER opendt

# Set working directory to service for proper module resolution
WORKDIR /app/services/k8s-decision-maker

CMD ["python", "-m", "k8s_decision_maker.main"]
