import { describe, it, expect, beforeAll, afterEach } from 'vitest';
import {
  runCLI,
  runCLIAndParseJSON,
  getTestDataPath,
  ensureBinaryBuilt,
} from './cli-helper.js';
import fs from 'fs';

describe('Binner CLI - Basic functionality', () => {
  const testOutputFile = 'test-output.json';

  beforeAll(async () => {
    // Ensure the binary is built
    await ensureBinaryBuilt();
  });

  afterEach(() => {
    // Clean up any output files
    if (fs.existsSync(testOutputFile)) {
      fs.unlinkSync(testOutputFile);
    }
  });

  it('should show help when run without arguments', async () => {
    const result = await runCLI(['--help']);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain('A CLI tool that reads numeric data');
    expect(result.stdout).toContain('--file');
    expect(result.stdout).toContain('--column');
    expect(result.stdout).toContain('--algorithm');
  });

  it('should list columns in the athletes parquet file', async () => {
    const result = await runCLI([
      '--list-columns',
      '-f',
      getTestDataPath('athletes.parquet'),
    ]);

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain('Available columns');
  });

  it('should fail gracefully with non-existent file', async () => {
    const result = await runCLI([
      '-f',
      'non-existent.parquet',
      '-c',
      'weight',
      '-a',
      'jenks',
    ]);

    expect(result.exitCode).not.toBe(0);
    expect(result.stderr).toContain('Error');
  });

  it('should save results to JSON file', async () => {
    const result = await runCLI([
      '-f',
      getTestDataPath('athletes.parquet'),
      '-c',
      'weight',
      '-a',
      'jenks',
      '-n',
      '3',
      '-o',
      testOutputFile,
    ]);

    expect(result.exitCode).toBe(0);
    expect(fs.existsSync(testOutputFile)).toBe(true);

    const savedData = JSON.parse(fs.readFileSync(testOutputFile, 'utf8'));
    expect(savedData).toHaveProperty('metadata');
    expect(savedData).toHaveProperty('bins');
    expect(savedData.metadata.algorithm).toBe('Jenks');
  });

  describe('Error handling', () => {
    it('should fail with invalid algorithm', async () => {
      const result = await runCLI([
        '-f',
        getTestDataPath('athletes.parquet'),
        '-c',
        'weight',
        '-a',
        'invalid-algorithm',
      ]);

      expect(result.exitCode).not.toBe(0);
    });

    it('should fail with non-existent column', async () => {
      const result = await runCLI([
        '-f',
        getTestDataPath('athletes.parquet'),
        '-c',
        'non_existent_column',
        '-a',
        'jenks',
      ]);

      expect(result.exitCode).not.toBe(0);
    });

    it('should fail with invalid number of bins', async () => {
      const result = await runCLI([
        '-f',
        getTestDataPath('athletes.parquet'),
        '-c',
        'weight',
        '-a',
        'jenks',
        '-n',
        '0',
      ]);

      expect(result.exitCode).not.toBe(0);
    });

    it('should fail with malformed custom bins', async () => {
      const result = await runCLI([
        '-f',
        getTestDataPath('athletes.parquet'),
        '-c',
        'weight',
        '--bins',
        'not,numeric,values',
      ]);

      expect(result.exitCode).not.toBe(0);
    });
  });

  describe('Data validation', () => {
    it('should report correct metadata', async () => {
      const result = await runCLIAndParseJSON([
        '-f',
        getTestDataPath('athletes.parquet'),
        '-c',
        'weight',
        '-a',
        'jenks',
        '-n',
        '5',
      ]);

      const { metadata } = result;

      expect(metadata.file).toContain('athletes.parquet');
      expect(metadata.column).toBe('weight');
      expect(metadata.total_rows).toBeGreaterThan(0);
      expect(metadata.numeric_values).toBeGreaterThan(0);
      expect(metadata.numeric_values).toBeLessThanOrEqual(metadata.total_rows);
      expect(metadata.null_values).toBeGreaterThanOrEqual(0);
      expect(metadata.null_values + metadata.numeric_values).toBe(
        metadata.total_rows
      );
    });

    it('should have consistent bin counts', async () => {
      const result = await runCLIAndParseJSON([
        '-f',
        getTestDataPath('athletes.parquet'),
        '-c',
        'weight',
        '-a',
        'quantile',
        '-n',
        '4',
      ]);

      const totalBinCount = result.bins.reduce(
        (sum, bin) => sum + bin.count,
        0
      );
      expect(totalBinCount).toBe(result.metadata.numeric_values);
    });

    it('should maintain proper bin ordering', async () => {
      const result = await runCLIAndParseJSON([
        '-f',
        getTestDataPath('athletes.parquet'),
        '-c',
        'weight',
        '-a',
        'equal-interval',
        '-n',
        '6',
      ]);

      // Filter out overflow/underflow bins for ordering check
      const dataBins = result.bins.filter(
        bin =>
          !bin.bin_label.includes('overflow') &&
          !bin.bin_label.includes('underflow')
      );

      for (let i = 1; i < dataBins.length; i++) {
        expect(dataBins[i].from).toBeGreaterThanOrEqual(dataBins[i - 1].to);
      }
    });
  });

  describe('Performance', () => {
    it('should complete binning in reasonable time', async () => {
      const startTime = Date.now();

      await runCLIAndParseJSON([
        '-f',
        getTestDataPath('athletes.parquet'),
        '-c',
        'weight',
        '-a',
        'jenks',
        '-n',
        '10',
      ]);

      const duration = Date.now() - startTime;
      expect(duration).toBeLessThan(10000); // Should complete within 10 seconds
    });
  });
});
