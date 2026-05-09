FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (cached layer — only rebuilds if requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files (always fresh — no layer caching for source files)
COPY . .

EXPOSE 8080

CMD ["python3", "server.py"]
