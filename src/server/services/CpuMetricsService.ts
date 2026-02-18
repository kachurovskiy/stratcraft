import os from 'os';

export interface CpuSample {
  ts: number;
  avg: number;
  min: number;
  max: number;
}

export interface CpuMetricsSummary {
  startedAt: number;
  uptimeMs: number;
  coreCount: number;
  sampleIntervalMs: number;
  samples: CpuSample[];
  latest?: CpuSample;
}

const DEFAULT_SAMPLE_INTERVAL_MS = 15000;
const DEFAULT_PRECISION = 1;

export class CpuMetricsService {
  private readonly startedAt: number = Date.now();
  private readonly sampleIntervalMs: number;
  private samples: CpuSample[] = [];
  private timer: NodeJS.Timeout | null = null;
  private primeTimer: NodeJS.Timeout | null = null;
  private lastTimes: os.CpuInfo['times'][] | null = null;
  private coreCount: number = 0;

  constructor(sampleIntervalMs: number = DEFAULT_SAMPLE_INTERVAL_MS) {
    this.sampleIntervalMs = sampleIntervalMs;
  }

  start(): void {
    if (this.timer) {
      return;
    }

    this.captureBaseline();

    const primeDelay = Math.min(1000, this.sampleIntervalMs);
    this.primeTimer = setTimeout(() => {
      this.captureSample();
    }, primeDelay);
    this.primeTimer.unref?.();

    this.timer = setInterval(() => {
      this.captureSample();
    }, this.sampleIntervalMs);
    this.timer.unref?.();
  }

  stop(): void {
    if (this.primeTimer) {
      clearTimeout(this.primeTimer);
      this.primeTimer = null;
    }
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  getSummary(maxPoints?: number): CpuMetricsSummary {
    const trimmed = typeof maxPoints === 'number' && Number.isFinite(maxPoints) && maxPoints > 0
      ? this.downsampleSamples(this.samples, Math.floor(maxPoints))
      : this.samples;

    return {
      startedAt: this.startedAt,
      uptimeMs: Date.now() - this.startedAt,
      coreCount: this.coreCount,
      sampleIntervalMs: this.sampleIntervalMs,
      samples: trimmed,
      latest: this.samples.length ? this.samples[this.samples.length - 1] : undefined
    };
  }

  private captureBaseline(): void {
    const cpus = os.cpus();
    this.coreCount = cpus.length;
    this.lastTimes = cpus.map(cpu => ({ ...cpu.times }));
  }

  private captureSample(): void {
    const sample = this.calculateSample();
    if (!sample) {
      return;
    }
    this.samples.push(sample);
  }

  private calculateSample(): CpuSample | null {
    const cpus = os.cpus();
    if (!this.lastTimes || this.lastTimes.length !== cpus.length) {
      this.captureBaseline();
      return null;
    }

    const perCoreUsage: number[] = [];
    for (let index = 0; index < cpus.length; index++) {
      const current = cpus[index].times;
      const previous = this.lastTimes[index];
      const totalDelta = totalCpuTime(current) - totalCpuTime(previous);
      const idleDelta = current.idle - previous.idle;
      const usage = totalDelta > 0 ? (1 - idleDelta / totalDelta) * 100 : 0;
      perCoreUsage.push(clamp(usage, 0, 100));
    }

    this.lastTimes = cpus.map(cpu => ({ ...cpu.times }));
    this.coreCount = cpus.length;

    if (!perCoreUsage.length) {
      return null;
    }

    const avg = perCoreUsage.reduce((sum, value) => sum + value, 0) / perCoreUsage.length;
    const min = Math.min(...perCoreUsage);
    const max = Math.max(...perCoreUsage);

    return {
      ts: Date.now(),
      avg: roundTo(avg, DEFAULT_PRECISION),
      min: roundTo(min, DEFAULT_PRECISION),
      max: roundTo(max, DEFAULT_PRECISION)
    };
  }

  private downsampleSamples(samples: CpuSample[], maxPoints: number): CpuSample[] {
    if (samples.length <= maxPoints) {
      return samples;
    }

    const bucketSize = Math.ceil(samples.length / maxPoints);
    const downsampled: CpuSample[] = [];

    for (let start = 0; start < samples.length; start += bucketSize) {
      const bucket = samples.slice(start, start + bucketSize);
      if (!bucket.length) {
        continue;
      }

      let sumAvg = 0;
      let min = Number.POSITIVE_INFINITY;
      let max = Number.NEGATIVE_INFINITY;

      for (const sample of bucket) {
        sumAvg += sample.avg;
        if (sample.min < min) min = sample.min;
        if (sample.max > max) max = sample.max;
      }

      downsampled.push({
        ts: bucket[bucket.length - 1].ts,
        avg: roundTo(sumAvg / bucket.length, DEFAULT_PRECISION),
        min: roundTo(min, DEFAULT_PRECISION),
        max: roundTo(max, DEFAULT_PRECISION)
      });
    }

    return downsampled;
  }
}

function totalCpuTime(times: os.CpuInfo['times']): number {
  return times.user + times.nice + times.sys + times.idle + times.irq;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function roundTo(value: number, precision: number): number {
  const factor = Math.pow(10, precision);
  return Math.round(value * factor) / factor;
}
