FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY simulation_service.py .

# Unbuffered output so logs appear immediately in SYSTEM$GET_SERVICE_LOGS
CMD ["python", "-u", "simulation_service.py"]
