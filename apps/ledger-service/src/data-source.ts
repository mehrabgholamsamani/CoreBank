import { DataSource } from 'typeorm';
import { InitialLedger1710000000010 } from './migrations/1710000000010-initial';
import { StageFourReservations1710000000011 } from './migrations/1710000000011-stage-four-reservations';
import { LedgerAccountLinks1710000000012 } from './migrations/1710000000012-account-links';
export const ledgerDataSource = new DataSource({
  type: 'postgres',
  url: process.env.DATABASE_URL,
  migrations: [
    InitialLedger1710000000010,
    StageFourReservations1710000000011,
    LedgerAccountLinks1710000000012,
  ],
  synchronize: false,
});
