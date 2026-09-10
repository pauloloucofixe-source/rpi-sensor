FROM python:3.12-slim

WORKDIR /app

# Adicionámos o swig e o liblgpio-dev a esta lista
RUN apt-get update && \
    apt-get install -y \
        python3-dev \
        gcc \
        libffi-dev \
        swig \
        liblgpio-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY controller.py .

ENV PYTHONUNBUFFERED=1
ENV GPIOZERO_PIN_FACTORY=lgpio

CMD ["python", "controller.py"]
