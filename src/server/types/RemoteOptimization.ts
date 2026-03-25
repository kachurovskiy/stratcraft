export type RemoteOptimizationStatus = 'queued' | 'running' | 'handoff' | 'succeeded' | 'failed';

export interface RemoteOptimizationRequest {
  templateId: string;
  templateName: string;
  triggeredBy: {
    userId: number | string;
    email: string;
  };
}

export interface RemoteOptimizationJobSnapshot {
  id: string;
  templateId: string;
  templateName: string;
  status: RemoteOptimizationStatus;
  createdAt: Date;
  startedAt?: Date;
  finishedAt?: Date;
  error?: string;
  remoteServerIp?: string;
  hetznerServerId?: number;
  triggeredBy: RemoteOptimizationRequest['triggeredBy'];
  resultSummary?: string;
  failureStage?: string;
  failureDetails?: string;
  currentStage?: string;
  remoteHandoffComplete?: boolean;
}
