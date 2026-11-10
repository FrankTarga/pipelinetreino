FROM python:3.12-slim

WORKDIR /app

# Set environment variables for Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install project dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY . .

# Run the pipeline by default
ENTRYPOINT ["python", "pipeline.py"]
CMD ["--storage", "sqlite"]
