FROM node:20-alpine AS build
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-workspace.yaml tsconfig.base.json nest-cli.json ./
COPY apps ./apps
COPY libs ./libs
RUN pnpm install --frozen-lockfile=false
RUN pnpm build
FROM node:20-alpine
WORKDIR /app
RUN corepack enable
COPY --from=build /app /app
ENV NODE_ENV=production
CMD ["node", "apps/gateway/dist/main.js"]
