FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y \
        python3-dev \
        gcc \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY controller.py .

ENV PYTHONUNBUFFERED=1
ENV GPIOZERO_PIN_FACTORY=lgpio

CMD ["python", "controller.py"]