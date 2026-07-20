# Telegram Clone

![CI](https://github.com/kntjspr/telegram-clone/actions/workflows/ci.yml/badge.svg)
![Docker](https://github.com/kntjspr/telegram-clone/actions/workflows/docker.yml/badge.svg)

A tool to clone Telegram channels, tracking cloned messages and providing a web interface.

## Installation

Using `uv`:
```bash
uv sync
```

Set up your `.env` file from `.env.example` with your Telegram API credentials and other settings.

## Usage

```bash
uv run src/telegram_clone/main.py
```

## Docker Usage

You can run the application using Docker on both Linux and Windows. Ensure you have Docker installed and running.

### 1. Build the image
**Linux / macOS:**
```bash
docker build -t telegram-clone .
```

**Windows (PowerShell):**
```powershell
docker build -t telegram-clone .
```

### 2. Run the container
You need to pass your `.env` file and mount a volume for your Telegram session and database files to persist data.

**Linux / macOS:**
```bash
docker run -it --rm \
  --env-file .env \
  -v $(pwd):/app \
  telegram-clone
```

**Windows (PowerShell):**
```powershell
docker run -it --rm `
  --env-file .env `
  -v ${PWD}:/app `
  telegram-clone
```
