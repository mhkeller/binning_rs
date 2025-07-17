/*
 * Binner - A CLI tool for creating histograms from Parquet files
 *
 * This tool reads numeric data from Parquet files and creates histogram bins
 * using various classification algorithms (Jenks, Quantile, Equal Interval, etc.)
 * Output is provided as JSON with metadata and bin statistics.
 */

use polars::prelude::*;
use classify::{get_jenks_classification, get_quantile_classification, get_equal_interval_classification, get_st_dev_classification, get_head_tail_classification};
use ndhistogram::{Histogram, ndhistogram, axis::Variable};
use clap::{Parser, ValueEnum};
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::{Write};

#[derive(Debug, Clone, ValueEnum)]
enum BinningAlgorithm {
    Jenks,
    Quantile,
    EqualInterval,
    StandardDeviation,
    HeadTail,
}

#[derive(Serialize, Deserialize)]
struct NumericHistogramBin {
    /// The label for this bin
    bin_label: String,
    /// The lower bound of the bin
    from: Option<f64>,
    /// The upper bound of the bin
    to: Option<f64>,
    /// The number of rows in this bin
    count: usize,
    /// The min value in this bin
    min: Option<f64>,
    /// The max value in this bin
    max: Option<f64>,
}

#[derive(Serialize, Deserialize)]
struct HistogramMetadata {
    file: String,
    column: String,
    algorithm: Option<String>,
    num_bins: Option<usize>,
    std_dev_size: Option<f64>,
    total_rows: usize,
    numeric_values: usize,
    null_values: usize,
    bin_edges: Vec<f64>,
}

#[derive(Serialize, Deserialize)]
struct HistogramResult {
    metadata: HistogramMetadata,
    bins: Vec<NumericHistogramBin>,
}

#[derive(Parser, Debug)]
#[command(
    name = "binner",
    author = "Data Analysis Tool",
    version = "1.0.0",
    about = "Create histograms from Parquet file data using various binning algorithms",
    long_about = "A CLI tool that reads numeric data from Parquet files and creates histogram bins using classification algorithms like Jenks, Quantile, Equal Interval, Standard Deviation, and Head-Tail. Output is provided as structured JSON."
)]
struct Args {
    /// Column name to analyze and bin
    #[arg(short, long, help = "Name of the numeric column to create histogram bins for")]
    column: Option<String>,

    /// Binning algorithm to use for automatic bin calculation
    #[arg(short, long, value_enum, help = "Algorithm for calculating bin boundaries")]
    algorithm: Option<BinningAlgorithm>,

    /// Number of bins to create (not used for HeadTail or Quantile algorithms)
    #[arg(short, long, default_value_t = 5, help = "Target number of bins to create")]
    num_bins: usize,

    /// Standard deviation multiplier (only for StandardDeviation algorithm)
    #[arg(long, default_value_t = 1.0, help = "Number of standard deviations for bin sizing")]
    std_dev_size: f64,

    /// Path to the Parquet file to analyze
    #[arg(short, long, help = "Path to the input Parquet file")]
    file: String,

    /// List all available columns in the Parquet file and exit
    #[arg(long, help = "Show available columns in the file and exit")]
    list_columns: bool,

    /// Custom bin edges as comma-separated values (alternative to algorithm-based binning)
    #[arg(long, value_delimiter = ',', help = "Custom bin boundaries (comma-separated). Use 'null' to include null values bin")]
    bins: Option<Vec<String>>,

    /// Output file path for JSON results (prints to stdout if not specified)
    #[arg(short, long, help = "File path to write JSON results (optional)")]
    output: Option<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // If list_columns is specified, just show the columns and exit
    if args.list_columns {
        let lf = LazyFrame::scan_parquet(&args.file, Default::default())?;
        let df = lf.limit(0).collect()?; // Just get schema, no data
        println!("Available columns in {}:", args.file);
        for (i, column_name) in df.get_column_names().iter().enumerate() {
            println!("  {}. {}", i + 1, column_name);
        }
        return Ok(());
    }

    // Ensure required arguments are provided when not listing columns
    let column = args.column.ok_or("Column name is required when not listing columns")?;

    // Algorithm is only required if custom bins are not provided
    if args.bins.is_none() && args.algorithm.is_none() {
        return Err("Either algorithm or custom bins must be provided".into());
    }

    // Read data using Polars lazy API
    let lf = LazyFrame::scan_parquet(&args.file, Default::default())?
        .select([col(&column)]);

    let df = lf.collect()?;

    // Extract the column and handle nulls
    let series = df.column(&column)?;
    let mut numeric_values = Vec::new();

    // Convert to ChunkedArray to iterate over values
    for i in 0..series.len() {
        if let Ok(av) = series.get(i) {
            match av {
                AnyValue::Float64(f) => numeric_values.push(f),
                AnyValue::Float32(f) => numeric_values.push(f as f64),
                AnyValue::Int64(i) => numeric_values.push(i as f64),
                AnyValue::Int32(i) => numeric_values.push(i as f64),
                AnyValue::Int16(i) => numeric_values.push(i as f64),
                AnyValue::Int8(i) => numeric_values.push(i as f64),
                AnyValue::UInt64(i) => numeric_values.push(i as f64),
                AnyValue::UInt32(i) => numeric_values.push(i as f64),
                AnyValue::UInt16(i) => numeric_values.push(i as f64),
                AnyValue::UInt8(i) => numeric_values.push(i as f64),
                _ => {}, // Skip nulls and non-numeric types
            }
        }
    }

    let null_count = df.height() - numeric_values.len();

    if numeric_values.is_empty() {
        eprintln!("Error: No numeric values found in column '{}'", column);
        std::process::exit(1);
    }

    // Use custom bins if provided, otherwise calculate bins using algorithm
    let algorithm_used = args.algorithm.clone();
    let (breaks, include_null_bin) = if let Some(custom_bins) = args.bins {

        // Parse bins and check for null
        let mut parsed_breaks = Vec::new();
        let mut has_null_bin = false;

        for bin_str in custom_bins {
            if bin_str.to_lowercase() == "null" {
                has_null_bin = true;
            } else {
                match bin_str.parse::<f64>() {
                    Ok(value) => parsed_breaks.push(value),
                    Err(_) => return Err(format!("Invalid bin value: '{}'. Use numeric values or 'null'", bin_str).into()),
                }
            }
        }

        // Sort the numeric breaks
        parsed_breaks.sort_by(|a, b| a.partial_cmp(b).unwrap());

        (parsed_breaks, has_null_bin)
    } else {
        let algorithm = args.algorithm.ok_or("Algorithm is required when custom bins are not provided")?;

        // Create the binning classification based on algorithm
        let bins = match algorithm {
            BinningAlgorithm::Jenks => {
                get_jenks_classification(args.num_bins, &numeric_values)
            },
            BinningAlgorithm::Quantile => {
                get_quantile_classification(args.num_bins, &numeric_values)
            },
            BinningAlgorithm::EqualInterval => {
                get_equal_interval_classification(args.num_bins, &numeric_values)
            },
            BinningAlgorithm::StandardDeviation => {
                get_st_dev_classification(args.std_dev_size, &numeric_values)
            },
            BinningAlgorithm::HeadTail => {
                get_head_tail_classification(&numeric_values)
            }
        };

        // Extract bin edges
        let mut calculated_breaks: Vec<f64> = Vec::new();

        // For classify library, extract the bin_start values from bins to create breaks
        for bin in &bins {
            calculated_breaks.push(bin.bin_start);
        }

        // Add the final edge to complete the bins
        if let Some(&max_val) = numeric_values.iter().max_by(|a, b| a.partial_cmp(b).unwrap()) {
            calculated_breaks.push(max_val + f64::EPSILON); // Add small epsilon to include max value
        }

        (calculated_breaks, false) // Algorithm-based bins don't include null bin by default
    };

    // Create histogram using ndhistogram with Variable axis
    // Variable axis automatically includes underflow and overflow bins
    let mut hist = ndhistogram!(Variable::new(breaks.clone())?);

    // Fill histogram with values
    for &value in &numeric_values {
        hist.fill(&value);
    }

    // Prepare metadata
    let metadata = HistogramMetadata {
        file: args.file.clone(),
        column: column.clone(),
        algorithm: algorithm_used.as_ref().map(|a| format!("{:?}", a)),
        num_bins: if algorithm_used.is_some() { Some(args.num_bins) } else { None },
        std_dev_size: if matches!(algorithm_used, Some(BinningAlgorithm::StandardDeviation)) {
            Some(args.std_dev_size)
        } else {
            None
        },
        total_rows: df.height(),
        numeric_values: numeric_values.len(),
        null_values: null_count,
        bin_edges: breaks.clone(),
    };

    // Build bins with min/max tracking
    let mut bins = Vec::new();

    for item in hist.iter() {
        let count = *item.value as usize;

        // Calculate min/max for this bin by filtering values
        let (min_val, max_val, bin_label, from, to) = match &item.bin {
            ndhistogram::axis::BinInterval::Underflow { end } => {
                let values_in_bin: Vec<f64> = numeric_values.iter()
                    .filter(|&&v| v < *end)
                    .cloned()
                    .collect();
                let min_val = values_in_bin.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).copied();
                let max_val = values_in_bin.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).copied();
                (min_val, max_val, format!("< {:.3}", end), None, Some(*end))
            },
            ndhistogram::axis::BinInterval::Overflow { start } => {
                let values_in_bin: Vec<f64> = numeric_values.iter()
                    .filter(|&&v| v >= *start)
                    .cloned()
                    .collect();
                let min_val = values_in_bin.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).copied();
                let max_val = values_in_bin.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).copied();
                (min_val, max_val, format!(">= {:.3}", start), Some(*start), None)
            },
            ndhistogram::axis::BinInterval::Bin { start, end } => {
                let values_in_bin: Vec<f64> = numeric_values.iter()
                    .filter(|&&v| v >= *start && v < *end)
                    .cloned()
                    .collect();
                let min_val = values_in_bin.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).copied();
                let max_val = values_in_bin.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).copied();
                (min_val, max_val, format!("[{:.3}, {:.3})", start, end), Some(*start), Some(*end))
            },
        };

        bins.push(NumericHistogramBin {
            bin_label,
            from,
            to,
            count,
            min: min_val,
            max: max_val,
        });
    }

    // Add null bin if needed
    if null_count > 0 && include_null_bin {
        bins.push(NumericHistogramBin {
            bin_label: "null".to_string(),
            from: None,
            to: None,
            count: null_count,
            min: None,
            max: None,
        });
    }

    let result = HistogramResult { metadata, bins };

    // Output results
    let json_output = serde_json::to_string_pretty(&result)?;

    if let Some(output_path) = args.output {
        let mut file = File::create(&output_path)?;
        file.write_all(json_output.as_bytes())?;
        eprintln!("Results written to {}", output_path);
    } else {
        println!("{}", json_output);
    }

    Ok(())
}
