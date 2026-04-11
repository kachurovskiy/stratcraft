import type { SettingsValue } from '../database/types';

export type SettingsRepo = {
  value: Pick<SettingsValue, 'paramScoring' | 'templateScoring'>;
};
