FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY schema.py .
COPY seed_data.py .
CMD ["python", "schema.py"]
