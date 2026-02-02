import './tracing';
import 'reflect-metadata';
import {
  All,
  Controller,
  Get,
  Header,
  HttpException,
  Module,
  Req,
  ServiceUnavailableException,
} from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { validateConfig } from '@corebank/config';
import { correlation, correlationIdFrom } from '@corebank/correlation';
import { logger } from '@corebank/logging';
import axios from 'axios';
import { Pool } from 'pg';
import type { NextFunction, Request, Response } from 'express';
const service = 'api-gateway';
const config = validateConfig(process.env);
const log = logger.child({ service });
const pool = new Pool({ connectionString: config.DATABASE_URL });
const rateWindow = new Map<string, { count: number; resetAt: number }>();
let requestCount = 0;
let errorCount = 0;
@Controller()
class StatusController {
  constructor(private readonly db: Pool) {}
  @Get('health') health() {
    return { status: 'ok', service };
  }
  @Get('metrics')
  @Header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8')
  metrics() {
    return `corebank_gateway_up 1\ncorebank_http_requests_total{service="api-gateway"} ${requestCount}\ncorebank_http_errors_total{service="api-gateway"} ${errorCount}\n`;
  }
  @Get('ready') async ready() {
    try {
      await this.db.query('select 1');
      return { status: 'ready', service };
    } catch {
      throw new ServiceUnavailableException({ status: 'not_ready', service });
    }
  }
}
const forward = async (target: string, request: Request) => {
  try {
    const response = await axios.request({
      url: `${target}${request.originalUrl}`,
      method: request.method,
      data: request.body,
      headers: {
        authorization: request.headers.authorization,
        'content-type': 'application/json',
        'x-correlation-id': request.headers['x-correlation-id']?.toString(),
      },
      validateStatus: () => true,
    });
    if (response.status >= 400) throw new HttpException(response.data, response.status);
    return response.data;
  } catch (error) {
    if (error instanceof HttpException) throw error;
    throw new HttpException('upstream unavailable', 503);
  }
};
@Controller()
class GatewayController {
  @All('auth/*') auth(@Req() request: Request) {
    return forward(process.env.IDENTITY_URL ?? 'http://identity-service:3001', request);
  }
  @All(['customers', 'accounts', 'accounts/*']) accounts(@Req() request: Request) {
    return forward(process.env.ACCOUNT_URL ?? 'http://account-service:3002', request);
  }
  @All('ledger/*') ledger(@Req() request: Request) {
    return forward(process.env.LEDGER_URL ?? 'http://ledger-service:3003', request);
  }
  @All(['payments', 'payments/*']) payments(@Req() request: Request) {
    return forward(process.env.PAYMENT_URL ?? 'http://payment-service:3004', request);
  }
  @All(['reconciliation/*']) reconciliation(@Req() request: Request) {
    return forward(process.env.RECONCILIATION_URL ?? 'http://reconciliation-service:3006', request);
  }
}
@Module({
  controllers: [StatusController, GatewayController],
  providers: [{ provide: Pool, useValue: pool }],
})
class AppModule {}
async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.use((request: Request, response: Response, next: NextFunction) => {
    const correlationId = correlationIdFrom(request.headers['x-correlation-id']);
    response.setHeader('x-correlation-id', correlationId);
    correlation.run({ correlationId }, next);
  });
  app.use((request: Request, response: Response, next: NextFunction) => {
    const started = Date.now();
    response.on('finish', () => {
      requestCount += 1;
      if (response.statusCode >= 400) errorCount += 1;
      log.info(
        {
          correlationId: correlation.get()?.correlationId,
          method: request.method,
          path: request.path,
          statusCode: response.statusCode,
          durationMs: Date.now() - started,
        },
        'http request completed',
      );
    });
    next();
  });
  app.use((request: Request, response: Response, next: NextFunction) => {
    response.setHeader('x-content-type-options', 'nosniff');
    response.setHeader('x-frame-options', 'DENY');
    response.setHeader('referrer-policy', 'no-referrer');
    response.setHeader('content-security-policy', "default-src 'none'");
    const key = request.ip ?? 'unknown';
    const now = Date.now();
    const entry = rateWindow.get(key);
    const current = !entry || entry.resetAt < now ? { count: 0, resetAt: now + 60_000 } : entry;
    current.count += 1;
    rateWindow.set(key, current);
    response.setHeader('x-ratelimit-limit', '120');
    response.setHeader('x-ratelimit-remaining', String(Math.max(0, 120 - current.count)));
    if (current.count > 120) throw new HttpException('rate limit exceeded', 429);
    next();
  });
  const document = SwaggerModule.createDocument(
    app,
    new DocumentBuilder()
      .setTitle('CoreBank Sandbox API')
      .setDescription(
        'Identity, account, ledger, and internal-payment APIs for the educational sandbox.',
      )
      .setVersion('0.4.0')
      .addBearerAuth()
      .build(),
  );
  SwaggerModule.setup('openapi', app, document);
  app.enableShutdownHooks();
  await app.listen(config.PORT);
  log.info({ port: config.PORT }, 'service started');
}
void bootstrap();
