FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
# Install huggingface/sentence-transformers deps
RUN pip install --no-cache-dir -r requirements.txt
COPY log_analyzer.py .
CMD ["python", "log_analyzer.py"]
