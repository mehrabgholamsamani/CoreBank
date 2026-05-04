import { correlation, correlationIdFrom } from './index';

describe('correlation', () => {
  it('propagates request metadata through the async context', () => {
    correlation.run({ correlationId: 'request-123' }, () => {
      expect(correlation.get()).toEqual({ correlationId: 'request-123' });
    });
  });

  it('uses a generated identifier for missing or invalid input', () => {
    expect(correlationIdFrom('valid-id_1')).toBe('valid-id_1');
    expect(correlationIdFrom('not valid')).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    );
  });
});
