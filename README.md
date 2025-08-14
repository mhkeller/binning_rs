# Binner - Histogram Binning Tool

A fast CLI tool for creating histograms from Parquet file data using various statistical binning algorithms.

## Features

- **Multiple Binning Algorithms**: Jenks, Quantile, Equal Interval, Standard Deviation, and Head-Tail
- **Custom Bins**: Define your own bin boundaries
- **Parquet Support**: Direct reading from Parquet files using Polars
- **JSON Output**: Structured output with metadata and statistics
- **Null Handling**: Automatic detection and optional binning of null values
- **Min/Max Tracking**: Per-bin minimum and maximum value tracking

## Installation

### Build from Source

```bash
# Install test dependencies (optional)
pnpm install

# Build the release binary
cargo build --release
```

### Testing

```bash
# Run all tests
pnpm test

# Run tests in watch mode
pnpm run test:watch

# Run tests with UI
pnpm run test:ui

# Run tests with coverage
pnpm run test:coverage
```

## Usage

### Basic Usage

```bash
# List available columns in a Parquet file
./target/release/binner_rs --list-columns -f data.parquet

# Create 5 bins using Jenks algorithm
./target/release/binner_rs -f data.parquet -c column_name -a jenks -n 5

# Save results to JSON file
./target/release/binner_rs -f data.parquet -c column_name -a jenks -n 5 -o results.json
```

### Algorithms

- **jenks**: Natural breaks (Jenks) algorithm for optimal data grouping using ckmeans
- **quantile**: Quantile-based binning (equal frequency bins)
- **equal-interval**: Equal-width bins across the data range
- **standard-deviation**: Bins based on standard deviation from mean
- **head-tail**: Head-tail breaks for heavy-tailed distributions

### Custom Bins

```bash
# Define custom bin boundaries
./target/release/binner_rs -f data.parquet -c column_name --bins 10,25,50,75,100

# Include null values as a separate bin
./target/release/binner_rs -f data.parquet -c column_name --bins 10,25,50,null
```

## Output Format

The tool outputs JSON with the following structure:

```json
{
  "metadata": {
    "file": "data.parquet",
    "column": "column_name",
    "algorithm": "Jenks",
    "num_bins": 5,
    "std_dev_size": null,
    "total_rows": 1000,
    "numeric_values": 950,
    "null_values": 50,
    "bin_edges": [0.0, 25.0, 50.0, 75.0, 100.0]
  },
  "bins": [
    {
      "bin_label": "[0.000, 25.000)",
      "from": 0.0,
      "to": 25.0,
      "count": 200,
      "min": 0.1,
      "max": 24.9
    }
  ]
}
```

## Options

- `-c, --column`: Name of the numeric column to analyze
- `-a, --algorithm`: Binning algorithm (jenks, quantile, equal-interval, standard-deviation, head-tail)
- `-n, --num-bins`: Number of bins to create (default: 5)
- `--std-dev-size`: Standard deviation multiplier for std-dev algorithm (default: 1.0)
- `-f, --file`: Path to Parquet file
- `--list-columns`: Show available columns and exit
- `--bins`: Custom bin boundaries (comma-separated)
- `-o, --output`: Output file path (optional, prints to stdout by default)

## Examples

```bash
# Jenks natural breaks with 4 bins
./target/release/binner_rs -f athletes.parquet -c weight -a jenks -n 4

# Quantile binning
./target/release/binner_rs -f athletes.parquet -c height -a quantile -n 5

# Standard deviation binning
./target/release/binner_rs -f data.parquet -c score -a standard-deviation --std-dev-size 2.0

# Custom bins with null handling
./target/release/binner_rs -f data.parquet -c price --bins 0,100,500,1000,null
```

## Dependencies

- **polars**: Fast DataFrame library for Parquet reading
- **classify**: Statistical classification algorithms
- **ckmeans**: Implementation of the Ckmeans.1d.dp algorithm for optimal k-means clustering in 1D
- **ndhistogram**: N-dimensional histogram with overflow/underflow handling
- **clap**: Command-line argument parsing
- **serde**: JSON serialization

## Performance

The tool is optimized for large datasets and leverages:
- Polars lazy evaluation for efficient data loading
- Rust's memory safety and zero-cost abstractions
- Efficient histogram algorithms from the classify crate
- Optimized ckmeans implementation for Jenks natural breaks classification
