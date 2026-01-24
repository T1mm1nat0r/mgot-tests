FROM python:3.12.1-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.8.14 /uv /uvx /bin/

ADD . /app
ARG GITHUB_TOKEN

# Set the working directory inside the container
WORKDIR /app

# install git
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# install all packages with uv
RUN uv sync 

# Set the command to run your application
CMD ["uv","run", "main.py"]
