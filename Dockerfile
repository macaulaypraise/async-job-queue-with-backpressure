FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir --default-timeout=100 "poetry>=2.1.3,<3.0.0"

COPY pyproject.toml poetry.lock* README.md ./

RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi


COPY . .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
