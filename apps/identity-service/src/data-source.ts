import { DataSource } from 'typeorm';
import { InitialIdentity1710000000000 } from './migrations/1710000000000-initial';
import { StageTwoIdentityRepair1710000000008 } from './migrations/1710000000008-stage-two-repair';
import { IdentityOutboxCompatibility1710000000012 } from './migrations/1710000000012-outbox-compatibility';
import { IdentityRefreshTokenCompatibility1710000000013 } from './migrations/1710000000013-refresh-token-compatibility';
import { RemovePlaintextRefreshTokens1710000000014 } from './migrations/1710000000014-remove-plaintext-refresh-tokens';
import { IdentityOutboxRetries1710000000015 } from './migrations/1710000000015-outbox-retries';
export const identityDataSource = new DataSource({
  type: 'postgres',
  url: process.env.DATABASE_URL,
  migrations: [
    InitialIdentity1710000000000,
    StageTwoIdentityRepair1710000000008,
    IdentityOutboxCompatibility1710000000012,
    IdentityRefreshTokenCompatibility1710000000013,
    RemovePlaintextRefreshTokens1710000000014,
    IdentityOutboxRetries1710000000015,
  ],
  synchronize: false,
});
