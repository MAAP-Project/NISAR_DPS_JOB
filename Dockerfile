FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libhdf5-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# copy your repo contents into /app
COPY . /app

# make run.sh executable
RUN chmod +x /app/run.sh

ENTRYPOINT ["/app/run.sh"]
