FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    ffmpeg \
    libnss3 libatk-bridge2.0-0 libcups2 libdrm2 \
    libdbus-1-3 libxkbcommon0 libxcomposite1 libxdamage1 \
    libxrandr2 libgbm1 libpango-1.0-0 libcairo2 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install playwright && playwright install --with-deps chromium
COPY . .
EXPOSE 5000
CMD ["python", "-m", "kick_promoter.web_ui.app"]
