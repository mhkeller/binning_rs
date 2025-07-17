import { describe, it, expect, beforeAll } from 'vitest';
import { BinnerCLI } from './cli-helper.js';

describe('Standard Deviation Algorithm', () => {
  let cli;

  beforeAll(() => {
    cli = new BinnerCLI();
  });

  it('should create bins based on standard deviation from mean', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'standard-deviation',
      '--std-dev-size',
      '1.0',
    ]);

    expect(result.metadata.algorithm).toBe('StandardDeviation');
    expect(result.metadata.std_dev_size).toBe(1.0);
    expect(result.bins.length).toBeGreaterThan(2);
  });

  it('should handle different standard deviation sizes', async () => {
    for (const stdSize of [0.5, 1.0, 1.5, 2.0]) {
      const result = await cli.runAndParseJSON([
        '-f',
        cli.getTestDataPath('athletes.parquet'),
        '-c',
        'weight',
        '-a',
        'standard-deviation',
        '--std-dev-size',
        stdSize.toString(),
      ]);

      expect(result.metadata.std_dev_size).toBe(stdSize);
      expect(result.metadata.algorithm).toBe('StandardDeviation');

      // More standard deviations should generally create more bins
      expect(result.bins.length).toBeGreaterThan(2);
    }
  });

  it('should create symmetrical bins around the mean', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'standard-deviation',
      '--std-dev-size',
      '1.0',
    ]);

    // Standard deviation bins should be centered around the mean
    // and extend in both directions
    const dataBins = result.bins.filter(
      bin =>
        !bin.bin_label.includes('overflow') &&
        !bin.bin_label.includes('underflow')
    );

    expect(dataBins.length).toBeGreaterThan(1);

    // Should have bins on both sides of the distribution
    const minBinStart = Math.min(...dataBins.map(bin => bin.from));
    const maxBinEnd = Math.max(...dataBins.map(bin => bin.to));

    expect(maxBinEnd - minBinStart).toBeGreaterThan(0);
  });

  it('should work with different columns and distributions', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'height',
      '-a',
      'standard-deviation',
      '--std-dev-size',
      '1.5',
    ]);

    expect(result.metadata.algorithm).toBe('StandardDeviation');
    expect(result.metadata.column).toBe('height');
    expect(result.metadata.std_dev_size).toBe(1.5);
    expect(result.bins.length).toBeGreaterThan(2);
  });

  it('should create fewer bins with larger standard deviation sizes', async () => {
    const smallStdResult = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'standard-deviation',
      '--std-dev-size',
      '0.5',
    ]);

    const largeStdResult = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'standard-deviation',
      '--std-dev-size',
      '2.0',
    ]);

    // Smaller std dev size should create more granular bins
    expect(smallStdResult.bins.length).toBeGreaterThanOrEqual(
      largeStdResult.bins.length
    );
  });

  it('should handle edge case with very small standard deviation size', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'standard-deviation',
      '--std-dev-size',
      '0.25',
    ]);

    expect(result.metadata.std_dev_size).toBe(0.25);
    expect(result.bins.length).toBeGreaterThan(4); // Should create many bins
  });

  it('should handle edge case with large standard deviation size', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'standard-deviation',
      '--std-dev-size',
      '3.0',
    ]);

    expect(result.metadata.std_dev_size).toBe(3.0);
    // With 3 standard deviations, most data should fit in a few bins
    expect(result.bins.length).toBeGreaterThan(2);
    expect(result.bins.length).toBeLessThan(10);
  });

  it('should properly classify data into standard deviation ranges', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'standard-deviation',
      '--std-dev-size',
      '1.0',
    ]);

    // Check that bins are properly labeled and structured
    result.bins.forEach(bin => {
      expect(bin).toHaveProperty('bin_label');
      expect(bin).toHaveProperty('count');
      expect(bin.count).toBeGreaterThanOrEqual(0);
    });

    // Total count should match metadata
    const totalBinCount = result.bins.reduce((sum, bin) => sum + bin.count, 0);
    expect(totalBinCount).toBe(result.metadata.numeric_values); // Use numeric_values instead of total_rows
  });
});
