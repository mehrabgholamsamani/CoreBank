const transitions = {
  ACTIVE: ['SUSPENDED', 'CLOSED'],
  SUSPENDED: ['ACTIVE', 'CLOSED'],
  CLOSED: [],
} as const;
describe('account status policy', () => {
  it('allows only explicit lifecycle transitions', () => {
    expect(transitions.ACTIVE).toContain('SUSPENDED');
    expect(transitions.CLOSED).toHaveLength(0);
  });
});
