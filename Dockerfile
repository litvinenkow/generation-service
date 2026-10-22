FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
RUN pip install --no-cache-dir . pytest pytest-asyncio
COPY app ./app
COPY migrations ./migrations
COPY tests ./tests
CMD ["sh", "-c", "python -m app.migrate && uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4"]
