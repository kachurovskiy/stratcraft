import os from 'os';

export interface Sample {
  ts: number;
  avg?: number;
  min?: number;
  max?: number;
  totalBytes?: number;
  freeBytes?: number;
  usedBytes?: number;
  usedPercent?: number;
}

export interface CpuMetricsSummary {
  startedAt: number;
  uptimeMs: number;
  coreCount: number;
  sampleIntervalMs: number;
  samples: Sample[];
  latest?: Sample;
}

export interface MemoryMetricsSummary {
  startedAt: number;
  uptimeMs: number;
  sampleIntervalMs: number;
  samples: Sample[];
  latest?: Sample;
}

const DEFAULT_SAMPLE_INTERVAL_MS = 15000;
const DEFAULT_PRECISION = 1;

export class CpuMetricsService {
  private readonly startedAt: number = Date.now();
  private readonly sampleIntervalMs: number;
  private samples: Sample[] = [];
  private memorySamples: Sample[] = [];
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

  getMemorySummary(maxPoints?: number): MemoryMetricsSummary {
    const trimmed = typeof maxPoints === 'number' && Number.isFinite(maxPoints) && maxPoints > 0
      ? this.downsampleMemorySamples(this.memorySamples, Math.floor(maxPoints))
      : this.memorySamples;

    return {
      startedAt: this.startedAt,
      uptimeMs: Date.now() - this.startedAt,
      sampleIntervalMs: this.sampleIntervalMs,
      samples: trimmed,
      latest: this.memorySamples.length ? this.memorySamples[this.memorySamples.length - 1] : undefined
    };
  }

  private captureBaseline(): void {
    const cpus = os.cpus();
    this.coreCount = cpus.length;
    this.lastTimes = cpus.map(cpu => ({ ...cpu.times }));
  }

  private captureSample(): void {
    const ts = Date.now();
    const cpuSample = this.calculateCpuSample(ts);
    if (!cpuSample) {
      return;
    }
    const memorySample = this.calculateMemorySample(ts);
    this.samples.push(cpuSample);
    if (memorySample) {
      this.memorySamples.push(memorySample);
    }
  }

  private calculateCpuSample(ts: number): Sample | null {
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
      ts,
      avg: roundTo(avg, DEFAULT_PRECISION),
      min: roundTo(min, DEFAULT_PRECISION),
      max: roundTo(max, DEFAULT_PRECISION)
    };
  }

  private calculateMemorySample(ts: number): Sample | null {
    const totalBytes = os.totalmem();
    if (!Number.isFinite(totalBytes) || totalBytes <= 0) {
      return null;
    }

    const freeBytes = Math.max(0, os.freemem());
    const usedBytes = Math.max(0, totalBytes - freeBytes);
    const usedPercent = (usedBytes / totalBytes) * 100;

    return {
      ts,
      totalBytes,
      freeBytes,
      usedBytes,
      usedPercent: roundTo(usedPercent, DEFAULT_PRECISION)
    };
  }

  private downsampleSamples(samples: Sample[], maxPoints: number): Sample[] {
    if (samples.length <= maxPoints) {
      return samples;
    }

    const bucketSize = Math.ceil(samples.length / maxPoints);
    const downsampled: Sample[] = [];

    for (let start = 0; start < samples.length; start += bucketSize) {
      const bucket = samples.slice(start, start + bucketSize);
      if (!bucket.length) {
        continue;
      }

      let sumAvg = 0;
      let min = Number.POSITIVE_INFINITY;
      let max = Number.NEGATIVE_INFINITY;

      for (const sample of bucket) {
        sumAvg += sample.avg ?? 0;
        if ((sample.min ?? 0) < min) min = sample.min ?? 0;
        if ((sample.max ?? 0) > max) max = sample.max ?? 0;
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

  private downsampleMemorySamples(samples: Sample[], maxPoints: number): Sample[] {
    if (samples.length <= maxPoints) {
      return samples;
    }

    const bucketSize = Math.ceil(samples.length / maxPoints);
    const downsampled: Sample[] = [];

    for (let start = 0; start < samples.length; start += bucketSize) {
      const bucket = samples.slice(start, start + bucketSize);
      if (!bucket.length) {
        continue;
      }

      let sumUsed = 0;
      let sumFree = 0;
      let sumPercent = 0;
      let totalBytes = bucket[bucket.length - 1].totalBytes ?? 0;

      for (const sample of bucket) {
        sumUsed += sample.usedBytes ?? 0;
        sumFree += sample.freeBytes ?? 0;
        sumPercent += sample.usedPercent ?? 0;
        if (typeof sample.totalBytes === 'number') {
          totalBytes = sample.totalBytes;
        }
      }

      const count = bucket.length;
      downsampled.push({
        ts: bucket[bucket.length - 1].ts,
        totalBytes,
        freeBytes: Math.round(sumFree / count),
        usedBytes: Math.round(sumUsed / count),
        usedPercent: roundTo(sumPercent / count, DEFAULT_PRECISION)
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
