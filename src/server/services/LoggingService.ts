import { Database } from '../database/Database';

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';
export type LogSource =
  | 'job-scheduler'
  | 'engine-cli'
  | 'engine-compile-job'
  | 'train-lightgbm-job'
  | 'market-data-job'
  | 'candle-job'
  | 'signal-job'
  | 'backtest-job'
  | 'reconcile-trades-job'
  | 'plan-operations-job'
  | 'dispatch-operations-job'
  | 'optimize-job'
  | 'remote-optimize'
  | 'admin'
  | 'StrategyManager'
  | 'auth'
  | 'templates'
  | 'webserver'
  | 'system'
  | 'account-data';

export interface LogEntry {
  id?: number;
  source: LogSource;
  level: LogLevel;
  message: string;
  metadata?: Record<string, any>;
  strategyId?: string;
  created_at?: Date;
}


export class LoggingService {
  private db: Database;
  private logBuffer: LogEntry[] = [];
  private bufferSize: number = 100;
  private flushInterval: number = 5000; // 5 seconds
  private flushTimer: NodeJS.Timeout | null = null;
  private isFlushing: boolean = false;

  constructor(database: Database) {
    this.db = database;
    this.startFlushTimer();
  }

  /**
   * Log a message with specified level and source
   */
  log(
    source: LogSource,
    level: LogLevel,
    message: string,
    metadata?: Record<string, any>,
    strategyId?: string
  ): void {
    const mergedMetadata = this.mergeMetadata(metadata, strategyId);
    const logEntry: LogEntry = {
      source,
      level,
      message,
      metadata: mergedMetadata,
      strategyId: strategyId ?? (mergedMetadata?.strategyId as string | undefined),
      created_at: new Date()
    };

    // Add to buffer for batch processing
    this.logBuffer.push(logEntry);

    // Also log to console for immediate visibility
    this.logToConsole(logEntry);

    // Flush buffer if it's full (fire and forget)
    if (this.logBuffer.length >= this.bufferSize) {
      this.flushLogs().catch(err => {
        console.error('Failed to flush logs:', err);
      });
    }
  }

  /**
   * Convenience methods for different log levels
   */
  debug(source: LogSource, message: string, metadata?: Record<string, any>, strategyId?: string): void {
    this.log(source, 'debug', message, metadata, strategyId);
  }

  info(source: LogSource, message: string, metadata?: Record<string, any>, strategyId?: string): void {
    this.log(source, 'info', message, metadata, strategyId);
  }

  warn(source: LogSource, message: string, metadata?: Record<string, any>, strategyId?: string): void {
    this.log(source, 'warn', message, metadata, strategyId);
  }

  error(source: LogSource, message: string, metadata?: Record<string, any>, strategyId?: string): void {
    this.log(source, 'error', message, metadata, strategyId);
  }


  /**
   * Get system logs with filtering
   */
  async getSystemLogs(
    source?: LogSource,
    level?: LogLevel,
    limit: number = 1000,
    offset: number = 0
  ): Promise<LogEntry[]> {
    try {
      const rows = await this.db.systemLogs.getSystemLogs({ source, level, limit, offset });
      return rows.map(row => this.mapRowToLogEntry(row));
    } catch (err) {
      console.error('Failed to get system logs:', err);
      return [];
    }
  }

  async getStrategyLogs(strategyId: string, limit: number = 100, offset: number = 0): Promise<LogEntry[]> {
    if (!strategyId) {
      return [];
    }

    try {
      const rows = await this.db.systemLogs.getSystemLogsByStrategyId(strategyId, limit, offset);
      return rows.map(row => this.mapRowToLogEntry(row));
    } catch (err) {
      console.warn('json_extract() unavailable for strategy logs, falling back to client-side filtering:', err);
      try {
        const rows = await this.db.systemLogs.getSystemLogs({ limit: limit * 4, offset });
        return rows
          .map(row => this.mapRowToLogEntry(row))
          .filter(entry => entry.strategyId === strategyId)
          .slice(0, limit);
      } catch (fallbackError) {
        console.error('Failed to load strategy logs:', fallbackError);
        return [];
      }
    }
  }

  async getJobLogs(jobId: string, limit: number = 1000, offset: number = 0): Promise<LogEntry[]> {
    if (!jobId) {
      return [];
    }

    try {
      const rows = await this.db.systemLogs.getSystemLogsByJobId(jobId, limit, offset);
      return rows.map(row => this.mapRowToLogEntry(row));
    } catch (err) {
      console.warn('Failed to query system logs by jobId, falling back to client-side filtering:', err);
      try {
        const rows = await this.db.systemLogs.getSystemLogs({ limit: limit * 4, offset });
        return rows
          .map(row => this.mapRowToLogEntry(row))
          .filter(entry => entry.metadata?.jobId === jobId)
          .slice(0, limit);
      } catch (fallbackError) {
        console.error('Failed to load job logs:', fallbackError);
        return [];
      }
    }
  }

  async getSystemLogSources(): Promise<string[]> {
    try {
      return await this.db.systemLogs.getSystemLogSources();
    } catch (error) {
      console.error('Failed to load system log sources:', error);
      return [];
    }
  }

  /**
   * Get logs grouped by source
   */
  async getLogsBySource(limit: number = 100): Promise<Record<string, LogEntry[]>> {
    try {
      const sources = await this.db.systemLogs.getSystemLogSources();
      if (!sources.length) {
        return {};
      }

      const logsBySource = await Promise.all(
        sources.map(async source => ({
          source,
          logs: await this.getSystemLogs(source as LogSource, undefined, limit)
        }))
      );

      return logsBySource.reduce((acc, entry) => {
        acc[entry.source] = entry.logs;
        return acc;
      }, {} as Record<string, LogEntry[]>);
    } catch (error) {
      console.error('Failed to load logs by source:', error);
      return {};
    }
  }

  /**
   * Clean up old logs (keep last 30 days)
   */
  async cleanupOldLogs(): Promise<void> {
    try {
      const thirtyDaysAgo = new Date();
      thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

      await this.db.systemLogs.deleteSystemLogsOlderThan(thirtyDaysAgo);

      this.info('system', `Cleaned up logs older than ${thirtyDaysAgo.toISOString()}`);
    } catch (err) {
      console.error('Failed to cleanup old logs:', err);
    }
  }

  /**
   * Delete every persisted system log (admin-only action)
   */
  async deleteAllLogs(): Promise<number> {
    try {
      await this.flushLogs();
      const deletedCount = await this.db.systemLogs.deleteAllSystemLogs();
      console.info(`Deleted ${deletedCount} system log entries via admin request`);
      return deletedCount;
    } catch (err) {
      console.error('Failed to delete all system logs:', err);
      throw err;
    }
  }

  /**
   * Flush buffered logs to database
   */
  private async flushLogs(): Promise<void> {
    if (this.logBuffer.length === 0 || this.isFlushing) return;

    this.isFlushing = true;
    const logsToFlush = [...this.logBuffer];
    try {
      for (const log of logsToFlush) {
        await this.db.systemLogs.insertSystemLog({
          source: log.source,
          level: log.level,
          message: log.message || 'No message',
          metadata: log.metadata ?? null,
          createdAt: log.created_at ?? new Date()
        });
      }
      this.logBuffer.splice(0, logsToFlush.length);
    } catch (err) {
      // We can wait to retry on next flush
    } finally {
      this.isFlushing = false;
    }
  }

  /**
   * Start the flush timer
   */
  private startFlushTimer(): void {
    this.flushTimer = setInterval(async () => {
      await this.flushLogs();
    }, this.flushInterval);
  }

  /**
   * Stop the flush timer and flush remaining logs
   */
  async shutdown(): Promise<void> {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    await this.flushLogs();
  }

  private mergeMetadata(
    metadata?: Record<string, any>,
    strategyId?: string
  ): Record<string, any> | undefined {
    if (!strategyId) {
      return metadata;
    }

    const merged: Record<string, any> = metadata ? { ...metadata } : {};
    if (!merged.strategyId) {
      merged.strategyId = strategyId;
    }
    return merged;
  }

  private parseMetadata(raw: any): Record<string, any> | undefined {
    if (!raw) {
      return undefined;
    }
    try {
      return typeof raw === 'string' ? JSON.parse(raw) : raw;
    } catch (err) {
      console.warn('Failed to parse log metadata payload:', err);
      return undefined;
    }
  }

  private mapRowToLogEntry(row: any): LogEntry {
    const metadata = this.parseMetadata(row.metadata);
    const strategyId =
      metadata && typeof metadata.strategyId === 'string' ? metadata.strategyId : undefined;

    return {
      id: typeof row.id === 'number' ? row.id : undefined,
      source: row.source as LogSource,
      level: row.level as LogLevel,
      message: row.message,
      metadata,
      strategyId,
      created_at: row.created_at ? new Date(row.created_at) : undefined
    };
  }

  /**
   * Log to console with appropriate formatting
   */
  private logToConsole(logEntry: LogEntry): void {
    const timestamp = new Date().toISOString();
    const levelColor = this.getLevelColor(logEntry.level);
    const sourceColor = this.getSourceColor(logEntry.source);

    const strategyContext = logEntry.strategyId ? ` (strategy ${logEntry.strategyId})` : '';
    const message = `${timestamp} [${logEntry.source}] ${logEntry.level.toUpperCase()}: ${logEntry.message}${strategyContext}`;

    if (logEntry.metadata) {
      console.log(message, logEntry.metadata);
    } else {
      console.log(message);
    }
  }

  private getLevelColor(level: LogLevel): string {
    switch (level) {
      case 'error': return '\x1b[31m'; // Red
      case 'warn': return '\x1b[33m';  // Yellow
      case 'info': return '\x1b[36m';  // Cyan
      case 'debug': return '\x1b[90m'; // Gray
      default: return '\x1b[0m';       // Reset
    }
  }

  private getSourceColor(source: LogSource): string {
    switch (source) {
      case 'job-scheduler': return '\x1b[35m'; // Magenta
      case 'engine-cli':
      case 'train-lightgbm-job':
      case 'engine-compile-job': return '\x1b[36m'; // Cyan
      case 'market-data-job': return '\x1b[34m'; // Blue
      case 'candle-job': return '\x1b[34m'; // Blue
      case 'signal-job': return '\x1b[32m'; // Green
      case 'backtest-job': return '\x1b[33m'; // Yellow
      case 'reconcile-trades-job': return '\x1b[96m'; // Bright cyan
      case 'plan-operations-job': return '\x1b[95m'; // Bright magenta
      case 'dispatch-operations-job': return '\x1b[94m'; // Bright blue
      case 'optimize-job': return '\x1b[31m'; // Red
      case 'StrategyManager': return '\x1b[33m'; // Yellow
      case 'auth': return '\x1b[33m'; // Yellow
      case 'webserver': return '\x1b[36m'; // Cyan
      case 'system': return '\x1b[37m'; // White
      default: return '\x1b[0m'; // Reset
    }
  }
}
