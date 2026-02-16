FROM python:3.11-slim

# System deps often needed by h5py
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libhdf5-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/
COPY run.sh .

RUN chmod +x /opt/app/run.sh

ENTRYPOINT ["/opt/app/run.sh"]
