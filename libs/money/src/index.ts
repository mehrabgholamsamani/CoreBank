export type Currency = 'EUR' | 'USD' | 'SEK';
export const currencies: readonly Currency[] = ['EUR', 'USD', 'SEK'];

/** Money is always represented as integer minor units, never floating point. */
export class Money {
  private constructor(
    readonly amountMinor: bigint,
    readonly currency: Currency,
  ) {
    if (amountMinor < 0n) throw new Error('Money amount cannot be negative');
  }
  static fromMinor(amount: string, currency: Currency): Money {
    if (!/^(0|[1-9]\d*)$/.test(amount))
      throw new Error('amount must be an integer minor-unit string');
    if (!currencies.includes(currency)) throw new Error('unsupported currency');
    return new Money(BigInt(amount), currency);
  }
  toJSON(): { amountMinor: string; currency: Currency } {
    return { amountMinor: this.amountMinor.toString(), currency: this.currency };
  }
}
