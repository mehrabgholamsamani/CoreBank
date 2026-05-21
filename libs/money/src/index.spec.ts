import { Money } from './index';

describe('Money', () => {
  it('accepts integer minor-unit strings and serializes bigint safely', () => {
    expect(Money.fromMinor('10000', 'EUR').toJSON()).toEqual({
      amountMinor: '10000',
      currency: 'EUR',
    });
  });
  it('rejects floating-point input', () => {
    expect(() => Money.fromMinor('1.50', 'EUR')).toThrow('integer minor-unit');
  });
});
