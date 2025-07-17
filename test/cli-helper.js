import { spawn, execSync } from 'child_process';
import path from 'path';

/**
 * Helper function to run the CLI binary
 * @param {string[]} args - Command line arguments
 * @returns {Promise<{exitCode: number, stdout: string, stderr: string}>}
 */
export async function runCLI(args = []) {
  const binPath = path.join(process.cwd(), 'target', 'release', 'binner_rs');

  return new Promise(resolve => {
    const child = spawn(binPath, args, {
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';

    child.stdout.on('data', data => {
      stdout += data.toString();
    });

    child.stderr.on('data', data => {
      stderr += data.toString();
    });

    child.on('close', exitCode => {
      resolve({
        exitCode,
        stdout,
        stderr,
      });
    });
  });
}

/**
 * Helper function to run CLI and parse JSON output
 * @param {string[]} args - Command line arguments
 * @returns {Promise<Object>} Parsed JSON object
 */
export async function runCLIAndParseJSON(args = []) {
  const result = await runCLI(args);
  if (result.exitCode !== 0) {
    throw new Error(
      `CLI failed with exit code ${result.exitCode}: ${result.stderr}`
    );
  }

  // Handle cases where output might contain debugging info before JSON
  let output = result.stdout.trim();

  // Find the JSON part of the output (starts with {)
  const jsonStart = output.indexOf('{');
  if (jsonStart === -1) {
    throw new Error(`No JSON found in output: ${output}`);
  }

  if (jsonStart > 0) {
    output = output.substring(jsonStart);
  }

  try {
    return JSON.parse(output);
  } catch (error) {
    throw new Error(
      `Failed to parse JSON: ${error.message}\nOutput: ${output}`
    );
  }
}

/**
 * Helper function to get test data path
 * @param {string} filename - Name of the test data file
 * @returns {string} Full path to the test data file
 */
export function getTestDataPath(filename) {
  return path.join(process.cwd(), 'test', filename);
}

/**
 * Ensure the binary is built before running tests
 */
export async function ensureBinaryBuilt() {
  try {
    execSync('cargo build --release', { cwd: process.cwd(), stdio: 'ignore' });
  } catch (error) {
    console.warn('Failed to build binary, assuming it exists');
  }
}

/**
 * CLI Wrapper
 */
export class BinnerCLI {
  async run(args) {
    return runCLI(args);
  }

  async runAndParseJSON(args) {
    return runCLIAndParseJSON(args);
  }

  getTestDataPath(filename) {
    return getTestDataPath(filename);
  }
}
