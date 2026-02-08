import { spawn, ChildProcess } from 'child_process';
import { existsSync } from 'fs';
import path from 'path';
import { LoggingService, LogSource } from '../services/LoggingService';

const ENGINE_SOURCE: LogSource = 'engine-cli';

interface EngineCliOptions {
  forceKillDelayMs?: number;
}

export class EngineCliService {
  private compilePromise: Promise<void> | null = null;
  private compiledAt: Date | null = null;
  private currentProcess: ChildProcess | null = null;
  private currentProcessLabel: string | null = null;
  private forceKillTimer: NodeJS.Timeout | null = null;
  private readonly forceKillDelayMs: number;

  constructor(
    private readonly loggingService: LoggingService,
    options: EngineCliOptions = {}
  ) {
    this.forceKillDelayMs = options.forceKillDelayMs ?? 10_000;
  }

  private mergeLogMetadata(
    logMetadata?: Record<string, unknown>,
    metadata?: Record<string, unknown>
  ): Record<string, unknown> | undefined {
    if (!logMetadata && !metadata) {
      return undefined;
    }
    return {
      ...(logMetadata ?? {}),
      ...(metadata ?? {})
    };
  }

  private get repoRoot(): string {
    return path.resolve(__dirname, '../../..');
  }

  private get engineDir(): string {
    return path.resolve(this.repoRoot, 'engine');
  }

  private get binaryPath(): string {
    const binaryName = process.platform === 'win32' ? 'engine.exe' : 'engine';
    return path.resolve(this.engineDir, 'target', 'release', binaryName);
  }

  isCompiled(): boolean {
    return existsSync(this.binaryPath);
  }

  async compile(
    abortSignal?: AbortSignal,
    logMetadata?: Record<string, unknown>
  ): Promise<void> {
    if (this.compilePromise) {
      return this.compilePromise;
    }

    this.compilePromise = new Promise<void>((resolve, reject) => {
      this.loggingService.info(
        ENGINE_SOURCE,
        'Compiling engine via cargo build --release',
        this.mergeLogMetadata(logMetadata)
      );
      const cargo = spawn('cargo', ['build', '--release'], {
        cwd: this.engineDir,
        env: process.env,
        stdio: ['ignore', 'pipe', 'pipe']
      });

      const cleanup = () => {
        abortSignal?.removeEventListener('abort', onAbort);
      };

      const onAbort = () => {
        cargo.kill('SIGTERM');
        cleanup();
        reject(new Error('Engine compilation aborted'));
      };

      if (abortSignal) {
        abortSignal.addEventListener('abort', onAbort);
      }

      cargo.stdout?.on('data', chunk => {
        const text = chunk.toString().trim();
        if (text) {
          this.loggingService.info(ENGINE_SOURCE, text, this.mergeLogMetadata(logMetadata));
        }
      });

      cargo.stderr?.on('data', chunk => {
        const text = chunk.toString().trim();
        if (text) {
          this.loggingService.warn(ENGINE_SOURCE, text, this.mergeLogMetadata(logMetadata));
        }
      });

      cargo.on('error', error => {
        cleanup();
        this.compilePromise = null;
        reject(error);
      });

      cargo.on('exit', (code) => {
        cleanup();
        this.compilePromise = null;
        if (code === 0) {
          this.compiledAt = new Date();
          this.loggingService.info(
            ENGINE_SOURCE,
            'Engine compilation completed',
            this.mergeLogMetadata(logMetadata, {
              compiledAt: this.compiledAt?.toISOString()
            })
          );
          resolve();
        } else {
          reject(new Error(`Cargo build exited with code ${code}`));
        }
      });
    });

    await this.compilePromise;
  }

  async run(
    mode: string,
    args: string[] = [],
    abortSignal?: AbortSignal,
    logMetadata?: Record<string, unknown>
  ): Promise<void> {
    if (!this.isCompiled()) {
      throw new Error('Engine binary is not available. Compile the engine before running CLI commands.');
    }

    await this.spawnProcess(this.binaryPath, [mode, ...args], abortSignal, logMetadata);
  }

  async runWithOutput(
    mode: string,
    args: string[] = [],
    abortSignal?: AbortSignal,
    logMetadata?: Record<string, unknown>
  ): Promise<{ stdout: string; stderr: string }> {
    if (!this.isCompiled()) {
      throw new Error('Engine binary is not available. Compile the engine before running CLI commands.');
    }

    return this.spawnProcessWithOutput(this.binaryPath, [mode, ...args], abortSignal, logMetadata);
  }

  forceTerminateActiveProcess(reason: string = 'force-terminate', logMetadata?: Record<string, unknown>): void {
    this.requestProcessTermination(reason, logMetadata);
  }

  private spawnProcess(
    command: string,
    args: string[],
    abortSignal?: AbortSignal,
    logMetadata?: Record<string, unknown>
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      const commandLabel = `${command} ${args.join(' ')}`.trim();
      this.loggingService.info(
        ENGINE_SOURCE,
        `Executing ${commandLabel}`,
        this.mergeLogMetadata(logMetadata, { command: commandLabel })
      );

      const child: ChildProcess = spawn(command, args, {
        cwd: this.engineDir,
        env: process.env,
        stdio: ['ignore', 'pipe', 'pipe']
      });

      this.currentProcess = child;
      this.currentProcessLabel = commandLabel;

      let settled = false;
      const resolveOnce = () => {
        if (!settled) {
          settled = true;
          resolve();
        }
      };
      const rejectOnce = (error: Error) => {
        if (!settled) {
          settled = true;
          reject(error);
        }
      };

      const detachAbortListener = () => {
        abortSignal?.removeEventListener('abort', onAbort);
      };

      const finalize = () => {
        detachAbortListener();
        this.clearForceKillTimer();
        this.currentProcess = null;
        this.currentProcessLabel = null;
      };

      const onAbort = () => {
        this.loggingService.warn(
          ENGINE_SOURCE,
          'Abort requested for engine command',
          this.mergeLogMetadata(logMetadata, { command: commandLabel })
        );
        this.requestProcessTermination('abort-signal', logMetadata);
        detachAbortListener();
        rejectOnce(new Error('Engine command aborted'));
      };

      if (abortSignal) {
        if (abortSignal.aborted) {
          onAbort();
          return;
        }
        abortSignal.addEventListener('abort', onAbort);
      }

      const dataParts: string[] = [];
      child.stdout?.on('data', chunk => {
        const text = chunk.toString().trim();
        if (text) {
          this.loggingService.info(ENGINE_SOURCE, text, this.mergeLogMetadata(logMetadata));
          dataParts.push(text);
        }
      });

      child.stderr?.on('data', chunk => {
        const text = chunk.toString().trim();
        if (text) {
          this.loggingService.warn(ENGINE_SOURCE, text, this.mergeLogMetadata(logMetadata));
          dataParts.push(text);
        }
      });

      child.on('error', error => {
        finalize();
        rejectOnce(error);
      });

      child.on('exit', code => {
        finalize();
        if (code === 0) {
          resolveOnce();
        } else {
          rejectOnce(new Error(`Engine CLI exited with code ${code}\n${dataParts.join('\n')}`));
        }
      });
    });
  }

  private spawnProcessWithOutput(
    command: string,
    args: string[],
    abortSignal?: AbortSignal,
    logMetadata?: Record<string, unknown>
  ): Promise<{ stdout: string; stderr: string }> {
    return new Promise((resolve, reject) => {
      const commandLabel = `${command} ${args.join(' ')}`.trim();
      this.loggingService.info(
        ENGINE_SOURCE,
        `Executing ${commandLabel}`,
        this.mergeLogMetadata(logMetadata, { command: commandLabel })
      );

      const child: ChildProcess = spawn(command, args, {
        cwd: this.engineDir,
        env: process.env,
        stdio: ['ignore', 'pipe', 'pipe']
      });

      this.currentProcess = child;
      this.currentProcessLabel = commandLabel;

      let settled = false;
      const resolveOnce = (result: { stdout: string; stderr: string }) => {
        if (!settled) {
          settled = true;
          resolve(result);
        }
      };
      const rejectOnce = (error: Error) => {
        if (!settled) {
          settled = true;
          reject(error);
        }
      };

      const detachAbortListener = () => {
        abortSignal?.removeEventListener('abort', onAbort);
      };

      const finalize = () => {
        detachAbortListener();
        this.clearForceKillTimer();
        this.currentProcess = null;
        this.currentProcessLabel = null;
      };

      const onAbort = () => {
        this.loggingService.warn(
          ENGINE_SOURCE,
          'Abort requested for engine command',
          this.mergeLogMetadata(logMetadata, { command: commandLabel })
        );
        this.requestProcessTermination('abort-signal', logMetadata);
        detachAbortListener();
        rejectOnce(new Error('Engine command aborted'));
      };

      if (abortSignal) {
        if (abortSignal.aborted) {
          onAbort();
          return;
        }
        abortSignal.addEventListener('abort', onAbort);
      }

      let stdout = '';
      let stderr = '';
      const dataParts: string[] = [];
      child.stdout?.on('data', chunk => {
        stdout += chunk.toString();
        const text = chunk.toString().trim();
        if (text) {
          this.loggingService.info(ENGINE_SOURCE, text, this.mergeLogMetadata(logMetadata));
          dataParts.push(text);
        }
      });

      child.stderr?.on('data', chunk => {
        stderr += chunk.toString();
        const text = chunk.toString().trim();
        if (text) {
          this.loggingService.warn(ENGINE_SOURCE, text, this.mergeLogMetadata(logMetadata));
          dataParts.push(text);
        }
      });

      child.on('error', error => {
        finalize();
        rejectOnce(error);
      });

      child.on('exit', code => {
        finalize();
        if (code === 0) {
          resolveOnce({ stdout, stderr });
        } else {
          rejectOnce(new Error(`Engine CLI exited with code ${code}\n${dataParts.join('\n')}`));
        }
      });
    });
  }

  private requestProcessTermination(reason: string, logMetadata?: Record<string, unknown>): void {
    const child = this.currentProcess;
    if (!child || child.killed) {
      return;
    }

    this.loggingService.warn(
      ENGINE_SOURCE,
      'Terminating engine process',
      this.mergeLogMetadata(logMetadata, {
        reason,
        command: this.currentProcessLabel ?? 'unknown'
      })
    );

    try {
      child.kill('SIGTERM');
    } catch (error) {
      this.loggingService.warn(
        ENGINE_SOURCE,
        'Failed to send SIGTERM to engine process',
        this.mergeLogMetadata(logMetadata, {
          error: error instanceof Error ? error.message : String(error)
        })
      );
    }

    if (!this.forceKillTimer) {
      this.forceKillTimer = setTimeout(() => {
        if (child.killed) {
          this.clearForceKillTimer();
          return;
        }

        this.loggingService.warn(
          ENGINE_SOURCE,
          'Force killing engine process after SIGTERM grace period',
          this.mergeLogMetadata(logMetadata, {
            command: this.currentProcessLabel ?? 'unknown'
          })
        );
        try {
          child.kill('SIGKILL');
        } catch {
          try {
            child.kill();
          } catch (killError) {
            this.loggingService.error(
              ENGINE_SOURCE,
              'Failed to force kill engine process',
              this.mergeLogMetadata(logMetadata, {
                error: killError instanceof Error ? killError.message : String(killError)
              })
            );
          }
        }
        this.clearForceKillTimer();
      }, this.forceKillDelayMs);
    }
  }

  private clearForceKillTimer(): void {
    if (this.forceKillTimer) {
      clearTimeout(this.forceKillTimer);
      this.forceKillTimer = null;
    }
  }
}
