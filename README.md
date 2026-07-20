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
