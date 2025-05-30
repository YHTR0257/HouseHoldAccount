FROM python:3.12-slim

WORKDIR /workspace

# Install dependencies, build tools, and git
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl \
    libffi-dev \
    postgresql-client \
    openssh-client \
    rsync \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt