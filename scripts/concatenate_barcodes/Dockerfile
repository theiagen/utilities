FROM python:3.13-slim

WORKDIR /data

# Install system dependencies and Google Cloud SDK
RUN apt-get update && apt-get install -y \
    gcc \
    libffi-dev \
    git \
    python3-distutils \
    libpq-dev \
    python3-dev \
    gnupg2 \
    curl \
    unzip \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
    && apt-get update \
    && apt-get install -y google-cloud-sdk \
    && rm -rf /var/lib/apt/lists/*

COPY scripts/concatenate_barcodes/concatenate-barcodes.py /app/

RUN chmod +x /app/*
ENV PATH="/app:${PATH}"

# Set environment variables for Google Cloud SDK
ENV CLOUDSDK_CONFIG=/gcp/config
ENV PYTHONPATH=/app

CMD ["concatenate-barcodes.py"]