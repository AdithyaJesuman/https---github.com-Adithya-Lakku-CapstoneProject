FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY features.py .
COPY detector.py .
# Will run detector by default, but typically run both via separate compose entries
CMD ["python", "detector.py"]
