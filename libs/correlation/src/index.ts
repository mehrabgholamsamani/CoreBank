import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';
export interface CorrelationContext {
  correlationId: string;
  causationId?: string;
  actorId?: string;
}
const storage = new AsyncLocalStorage<CorrelationContext>();
export const correlation = {
  run: <T>(context: CorrelationContext, callback: () => T) => storage.run(context, callback),
  get: () => storage.getStore(),
};

export const correlationIdFrom = (value: string | string[] | undefined): string => {
  const candidate = Array.isArray(value) ? value[0] : value;
  return candidate && /^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$/.test(candidate)
    ? candidate
    : randomUUID();
};
