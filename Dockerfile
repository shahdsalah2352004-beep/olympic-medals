FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.docker.txt /app/requirements.docker.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.docker.txt

COPY . /app

EXPOSE 8050

CMD ["python", "dashboard/app.py"]
