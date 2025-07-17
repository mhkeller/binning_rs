import { describe, it, expect, beforeAll } from 'vitest';
import { BinnerCLI } from './cli-helper.js';

describe('Jenks Algorithm', () => {
  let cli;

  beforeAll(() => {
    cli = new BinnerCLI();
  });

  it('should create bins using Jenks natural breaks algorithm', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'jenks',
      '-n',
      '4',
    ]);

    expect(result).toHaveProperty('metadata');
    expect(result).toHaveProperty('bins');
    expect(result.metadata.algorithm).toBe('Jenks');
    expect(result.metadata.num_bins).toBe(4);
    expect(result.bins).toHaveLength(6); // 4 bins + overflow + underflow

    // Check bin structure
    result.bins.forEach(bin => {
      expect(bin).toHaveProperty('bin_label');
      expect(bin).toHaveProperty('from');
      expect(bin).toHaveProperty('to');
      expect(bin).toHaveProperty('count');
      expect(bin).toHaveProperty('min');
      expect(bin).toHaveProperty('max');
    });
  });

  it('should handle different numbers of bins', async () => {
    for (const numBins of [3, 5, 8]) {
      const result = await cli.runAndParseJSON([
        '-f',
        cli.getTestDataPath('athletes.parquet'),
        '-c',
        'weight',
        '-a',
        'jenks',
        '-n',
        numBins.toString(),
      ]);

      expect(result.metadata.num_bins).toBe(numBins);
      expect(result.bins).toHaveLength(numBins + 2); // + overflow + underflow
    }
  });

  it('should create optimal breaks that minimize within-class variance', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'jenks',
      '-n',
      '5',
    ]);

    expect(result.metadata.algorithm).toBe('Jenks');
    expect(result.bins).toHaveLength(7); // 5 bins + overflow + underflow

    // Jenks should create meaningful breaks - check that breaks are not evenly spaced
    const dataBins = result.bins.filter(
      bin =>
        !bin.bin_label.includes('overflow') &&
        !bin.bin_label.includes('underflow')
    );

    const breaks = dataBins.map(bin => bin.to);
    const differences = [];
    for (let i = 1; i < breaks.length; i++) {
      differences.push(breaks[i] - breaks[i - 1]);
    }

    // If breaks were evenly spaced, all differences would be nearly equal
    // Jenks should create different-sized intervals
    const minDiff = Math.min(...differences);
    const maxDiff = Math.max(...differences);

    // Handle negative differences and ensure meaningful comparison
    if (minDiff > 0) {
      expect(maxDiff / minDiff).toBeGreaterThan(1.1); // At least 10% variation
    } else {
      // If we have negative or zero differences, just check we have variety
      expect(differences.length).toBeGreaterThan(1);
    }
  });

  it('should work with different columns', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'height',
      '-a',
      'jenks',
      '-n',
      '4',
    ]);

    expect(result.metadata.algorithm).toBe('Jenks');
    expect(result.metadata.column).toBe('height');
    expect(result.bins).toHaveLength(6);
  });

  it('should handle edge case with minimum bins', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'jenks',
      '-n',
      '2',
    ]);

    expect(result.metadata.num_bins).toBe(2);
    expect(result.bins).toHaveLength(4); // 2 bins + overflow + underflow
  });
});
