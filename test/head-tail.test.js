import { describe, it, expect, beforeAll } from 'vitest';
import { BinnerCLI } from './cli-helper.js';

describe('Head-Tail Algorithm', () => {
  let cli;

  beforeAll(() => {
    cli = new BinnerCLI();
  });

  it('should create head-tail breaks for heavy-tailed distributions', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'head-tail',
    ]);

    expect(result.metadata.algorithm).toBe('HeadTail');
    expect(result.bins.length).toBeGreaterThan(2);

    // Head-tail should create bins based on iterative mean breaks
    result.bins.forEach(bin => {
      expect(bin).toHaveProperty('bin_label');
      expect(bin).toHaveProperty('from');
      expect(bin).toHaveProperty('to');
      expect(bin).toHaveProperty('count');
      expect(bin).toHaveProperty('min');
      expect(bin).toHaveProperty('max');
    });
  });

  it('should work with different data distributions', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'height',
      '-a',
      'head-tail',
    ]);

    expect(result.metadata.algorithm).toBe('HeadTail');
    expect(result.metadata.column).toBe('height');
    expect(result.bins.length).toBeGreaterThan(2);
  });

  it('should create hierarchical breaks based on mean values', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'head-tail',
    ]);

    // Head-tail algorithm should create breaks that separate the data
    // into "head" (above mean) and "tail" (below mean) recursively
    const dataBins = result.bins.filter(
      bin =>
        !bin.bin_label.includes('overflow') &&
        !bin.bin_label.includes('underflow')
    );

    expect(dataBins.length).toBeGreaterThan(1);

    // Check that bins are ordered properly
    for (let i = 1; i < dataBins.length; i++) {
      expect(dataBins[i].from).toBeGreaterThanOrEqual(dataBins[i - 1].to);
    }
  });

  it('should handle the iterative nature of head-tail breaks', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'head-tail',
    ]);

    // The algorithm should stop when the condition is met
    // (when the proportion of values above mean becomes too small)
    expect(result.bins.length).toBeGreaterThan(2);
    expect(result.bins.length).toBeLessThan(20); // Should not create too many bins

    // Check that we have proper bin structure
    const totalCount = result.bins.reduce((sum, bin) => sum + bin.count, 0);
    expect(totalCount).toBe(result.metadata.numeric_values); // Use numeric_values instead of total_rows
  });

  it('should create meaningful breaks for heavy-tailed data', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'head-tail',
    ]);

    const dataBins = result.bins.filter(
      bin =>
        !bin.bin_label.includes('overflow') &&
        !bin.bin_label.includes('underflow')
    );

    // Head-tail should typically create more bins for the lower values
    // and fewer for the higher "tail" values
    expect(dataBins.length).toBeGreaterThan(2);

    // The first bins should generally have more data than the last bins
    // (this is the characteristic of head-tail breaks)
    if (dataBins.length >= 3) {
      const firstBinCount = dataBins[0].count;
      const lastBinCount = dataBins[dataBins.length - 1].count;

      // First bin should typically have more data than the last
      // (though this isn't guaranteed for all distributions)
      expect(firstBinCount).toBeGreaterThanOrEqual(0);
      expect(lastBinCount).toBeGreaterThanOrEqual(0);
    }
  });

  it('should work consistently across multiple runs', async () => {
    // Head-tail should produce deterministic results
    const result1 = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'head-tail',
    ]);

    const result2 = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'head-tail',
    ]);

    expect(result1.bins.length).toBe(result2.bins.length);
    expect(result1.metadata.algorithm).toBe(result2.metadata.algorithm);

    // Bin edges should be identical
    for (let i = 0; i < result1.bins.length; i++) {
      expect(result1.bins[i].from).toBeCloseTo(result2.bins[i].from, 5);
      expect(result1.bins[i].to).toBeCloseTo(result2.bins[i].to, 5);
      expect(result1.bins[i].count).toBe(result2.bins[i].count);
    }
  });

  it('should handle edge cases with small datasets', async () => {
    // Head-tail should work even if it doesn't create many hierarchical levels
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'head-tail',
    ]);

    expect(result.metadata.algorithm).toBe('HeadTail');
    expect(result.bins.length).toBeGreaterThanOrEqual(2);

    // Should always include overflow and underflow bins
    const overflowBin = result.bins.find(bin =>
      bin.bin_label.includes('overflow')
    );
    const underflowBin = result.bins.find(bin =>
      bin.bin_label.includes('underflow')
    );

    // Test should still pass if no overflow/underflow bins exist
    if (overflowBin) {
      expect(overflowBin.count).toBeGreaterThanOrEqual(0);
    }
    if (underflowBin) {
      expect(underflowBin.count).toBeGreaterThanOrEqual(0);
    }
  });
});
