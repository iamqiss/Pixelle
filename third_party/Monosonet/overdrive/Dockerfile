FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
	build-essential \
	curl \
	&& rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY overdrive ./overdrive

ENV OVERDRIVE_SERVICE_PORT=8088
EXPOSE 8088

CMD ["python", "-c", "from overdrive.app import run; run()"]