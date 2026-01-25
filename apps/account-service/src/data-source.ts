import { DataSource } from 'typeorm';
import { InitialAccount1710000000001 } from './migrations/1710000000001-initial';
import { StageTwoAccountRepair1710000000009 } from './migrations/1710000000009-stage-two-repair';
export const accountDataSource = new DataSource({
  type: 'postgres',
  url: process.env.DATABASE_URL,
  migrations: [InitialAccount1710000000001, StageTwoAccountRepair1710000000009],
  synchronize: false,
});
