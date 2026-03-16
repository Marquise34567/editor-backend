FROM node:22-bookworm-slim

ENV NODE_ENV=production

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    python3 \
    python3-pip \
    python3-venv \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./
COPY prisma ./prisma
COPY scripts ./scripts
RUN npm ci --include=dev

COPY . ./
RUN npm run build
RUN npm prune --omit=dev

EXPOSE 3000

CMD ["node", "scripts/start.js"]
