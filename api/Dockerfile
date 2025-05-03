# api/Dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT 8000

CMD ["uvicorn", "rag_api:app", "--host", "0.0.0.0", "--port", "$PORT"] 