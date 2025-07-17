import { describe, it, expect, beforeAll } from 'vitest';
import { BinnerCLI } from './cli-helper.js';

describe('Custom Bins Algorithm', () => {
  let cli;

  beforeAll(() => {
    cli = new BinnerCLI();
  });

  it('should create custom bins from provided boundaries', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '--bins',
      '50,70,90,110',
    ]);

    expect(result.metadata.algorithm).toBe(null); // Custom bins show null algorithm
    expect(result.bins).toHaveLength(5); // 3 bins + overflow + underflow (or just the data bins)

    // Check that the bin edges match our input
    const edges = result.metadata.bin_edges;
    expect(edges).toEqual([50, 70, 90, 110]);
  });

  it('should handle custom bins with null handling', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '--bins',
      '60,80,100,null',
    ]);

    // Should have regular bins plus null bin plus overflow/underflow
    expect(result.bins.length).toBeGreaterThan(4);

    // Check for null bin
    const nullBin = result.bins.find(bin => bin.bin_label.includes('null'));
    expect(nullBin).toBeDefined();
    expect(nullBin.count).toBeGreaterThanOrEqual(0);
  });

  it('should create bins with exact specified boundaries', async () => {
    const customBreaks = [40, 60, 80, 100, 120];
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '--bins',
      customBreaks.join(','),
    ]);

    expect(result.metadata.bin_edges).toEqual(customBreaks);

    const dataBins = result.bins.filter(
      bin =>
        !bin.bin_label.includes('overflow') &&
        !bin.bin_label.includes('underflow') &&
        !bin.bin_label.includes('null')
    );

    // Should have one bin for each interval between breaks plus overflow/underflow
    expect(dataBins.length).toBeGreaterThanOrEqual(customBreaks.length - 1);

    // Check that bin boundaries match our specification (within tolerance for null values)
    for (let i = 0; i < dataBins.length; i++) {
      if (dataBins[i].from !== null && customBreaks[i] !== undefined) {
        expect(dataBins[i].from).toBeCloseTo(customBreaks[i], 1);
      }
      if (dataBins[i].to !== null && customBreaks[i + 1] !== undefined) {
        expect(dataBins[i].to).toBeCloseTo(customBreaks[i + 1], 1);
      }
    }
  });

  it('should handle single break point', async () => {
    // Single break point should be invalid and throw an error
    await expect(
      cli.runAndParseJSON([
        '-f',
        cli.getTestDataPath('athletes.parquet'),
        '-c',
        'weight',
        '--bins',
        '75',
      ])
    ).rejects.toThrow('InvalidNumberOfBinEdges');
  });

  it('should handle many custom breaks', async () => {
    const manyBreaks = [30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130];
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '--bins',
      manyBreaks.join(','),
    ]);

    expect(result.metadata.bin_edges).toEqual(manyBreaks);

    const dataBins = result.bins.filter(
      bin =>
        !bin.bin_label.includes('overflow') &&
        !bin.bin_label.includes('underflow')
    );

    expect(dataBins.length).toBeGreaterThanOrEqual(manyBreaks.length - 1);
  });

  it('should work with unsorted break points', async () => {
    // The tool should handle unsorted breaks gracefully
    const unsortedBreaks = '90,50,110,70';
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '--bins',
      unsortedBreaks,
    ]);

    expect(result.metadata.algorithm).toBe(null); // Custom bins show null algorithm
    // Should sort the breaks internally
    expect(result.metadata.bin_edges).toEqual([50, 70, 90, 110]);
  });

  it('should handle custom bins with different data types', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'height',
      '--bins',
      '160,170,180,190',
    ]);

    expect(result.metadata.algorithm).toBe(null); // Custom bins show null algorithm
    expect(result.metadata.column).toBe('height');
    expect(result.metadata.bin_edges).toEqual([160, 170, 180, 190]);
  });

  it('should handle floating point break values', async () => {
    const floatBreaks = [55.5, 67.3, 78.9, 95.1];
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '--bins',
      floatBreaks.join(','),
    ]);

    expect(result.metadata.bin_edges).toEqual(floatBreaks);

    const dataBins = result.bins.filter(
      bin =>
        !bin.bin_label.includes('overflow') &&
        !bin.bin_label.includes('underflow')
    );

    for (let i = 0; i < dataBins.length; i++) {
      if (dataBins[i].from !== null) {
        expect(dataBins[i].from).toBeCloseTo(floatBreaks[i], 1);
      }
      if (dataBins[i].to !== null) {
        expect(dataBins[i].to).toBeCloseTo(floatBreaks[i + 1], 1);
      }
    }
  });

  it('should properly count data in custom bins', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '--bins',
      '60,80,100',
    ]);

    // Check that all data is accounted for
    const totalCount = result.bins.reduce((sum, bin) => sum + bin.count, 0);
    expect(totalCount).toBe(result.metadata.numeric_values); // Use numeric_values instead of total_rows

    // Each bin should have appropriate counts
    result.bins.forEach(bin => {
      expect(bin.count).toBeGreaterThanOrEqual(0);
      if (bin.count > 0) {
        expect(bin.min).toBeDefined();
        expect(bin.max).toBeDefined();
        expect(bin.min).toBeLessThanOrEqual(bin.max);
      }
    });
  });

  it('should handle custom bins that encompass the entire data range', async () => {
    const result = await cli.runAndParseJSON([
      '-f',
      cli.getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '--bins',
      '0,50,100,200',
    ]);

    // With breaks that likely encompass all data, overflow/underflow may not be needed
    const overflowBin = result.bins.find(bin =>
      bin.bin_label.includes('overflow')
    );
    const underflowBin = result.bins.find(bin =>
      bin.bin_label.includes('underflow')
    );

    // If overflow/underflow bins exist, they should have minimal counts
    if (overflowBin) {
      expect(overflowBin.count).toBeGreaterThanOrEqual(0);
    }
    if (underflowBin) {
      expect(underflowBin.count).toBeGreaterThanOrEqual(0);
    }

    // Most data should be in the regular bins
    const dataBins = result.bins.filter(
      bin =>
        !bin.bin_label.includes('overflow') &&
        !bin.bin_label.includes('underflow')
    );

    const dataInRegularBins = dataBins.reduce((sum, bin) => sum + bin.count, 0);
    expect(dataInRegularBins).toBeGreaterThan(0);
  });
});
