import './tracing';
import 'reflect-metadata';
import { Controller, Get, Module, ServiceUnavailableException } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { validateConfig } from '@corebank/config';
import { correlation } from '@corebank/correlation';
import { logger } from '@corebank/logging';
import { Pool } from 'pg';
import { randomUUID } from 'node:crypto';
import type { NextFunction, Request, Response } from 'express';

const service = 'notification-service';
const log = logger.child({ service });
const config = validateConfig(process.env);
const pool = new Pool({ connectionString: config.DATABASE_URL });
@Controller()
class StatusController {
  constructor(private readonly pool: Pool) {}
  @Get('health') health() {
    return { status: 'ok', service };
  }
  @Get('metrics') metrics() {
    return 'corebank_service_up{service="notification-service"} 1\n';
  }
  @Get('ready') async ready() {
    try {
      await this.pool.query('select 1');
      return { status: 'ready', service };
    } catch {
      throw new ServiceUnavailableException({ status: 'not_ready', service });
    }
  }
}
@Module({ controllers: [StatusController], providers: [{ provide: Pool, useValue: pool }] })
class AppModule {}
async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.use((request: Request, response: Response, next: NextFunction) => {
    const correlationId = request.headers['x-correlation-id']?.toString() ?? randomUUID();
    response.setHeader('x-correlation-id', correlationId);
    correlation.run({ correlationId }, next);
  });
  app.enableShutdownHooks();
  await app.listen(config.PORT);
  log.info({ port: config.PORT }, 'service started');
}
void bootstrap();
