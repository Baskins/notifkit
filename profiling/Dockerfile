FROM node:22-alpine

WORKDIR /app

# Dependencies first, so rebuilding after a code change reuses this layer.
COPY package*.json tsconfig.json tsdown.config.ts ./
COPY packages/ ./packages/
RUN npm install --ignore-scripts

WORKDIR /app/profiling
COPY profiling/package*.json profiling/tsconfig.json ./
RUN npm install

# Root package: sources plus the dist/ the runner builds right before this.
WORKDIR /app
COPY src/ ./src/
COPY drizzle/ ./drizzle/
COPY dist/ ./dist/

WORKDIR /app/profiling
COPY profiling/src/ ./src/

EXPOSE 3000

# node directly rather than `npm start`, so signals reach the process.
CMD ["node", "--import", "tsx", "src/server.ts"]
