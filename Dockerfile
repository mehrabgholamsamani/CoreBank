FROM node:20-alpine AS build
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.base.json nest-cli.json ./
COPY apps ./apps
COPY libs ./libs
RUN pnpm install --frozen-lockfile
RUN pnpm build
FROM node:20-alpine
WORKDIR /app
RUN corepack enable
COPY --from=build /app /app
ENV NODE_ENV=production
CMD ["node", "apps/api-gateway/dist/main.js"]
