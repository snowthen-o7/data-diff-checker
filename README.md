# Data Diff Checker

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A memory-optimized CSV diff tool with streaming processing for comparing large datasets between environments.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║   ██████╗  █████╗ ████████╗ █████╗     ██████╗ ██╗███████╗███████╗           ║
║   ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗    ██╔══██╗██║██╔════╝██╔════╝           ║
║   ██║  ██║███████║   ██║   ███████║    ██║  ██║██║█████╗  █████╗             ║
║   ██║  ██║██╔══██║   ██║   ██╔══██║    ██║  ██║██║██╔══╝  ██╔══╝             ║
║   ██████╔╝██║  ██║   ██║   ██║  ██║    ██████╔╝██║██║     ██║                ║
║   ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝    ╚═════╝ ╚═╝╚═╝     ╚═╝                ║
║            ██████╗██╗  ██╗███████╗ ██████╗██╗  ██╗███████╗██████╗            ║
║           ██╔════╝██║  ██║██╔════╝██╔════╝██║ ██╔╝██╔════╝██╔══██╗           ║
║           ██║     ███████║█████╗  ██║     █████╔╝ █████╗  ██████╔╝           ║
║           ╚██████╗██║  ██║███████╗╚██████╗██║  ██╗███████╗██║  ██║           ║
║            ╚═════╝╚═╝  ╚═╝╚══════╝ ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝           ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

## Features

- **Memory Efficient**: True streaming CSV processing - never loads entire files into memory
- **Fast Comparison**: Hash-based row comparison with two-pass algorithm
- **Composite Keys**: Support for multi-column primary keys
- **Smart Detection**: Auto-detects CSV delimiters and escape styles
- **Parallel Processing**: Concurrent fetches and diffs with configurable parallelism
- **Rich Output**: Detailed JSON reports with example IDs and line numbers
- **Cross-Platform**: Works on Linux, macOS, and Windows (with ANSI support)

## Installation

### From GitHub

```bash
# Clone the repository
git clone https://github.com/snowthen-o7/data-diff-checker.git
cd data-diff-checker

# Upgrade pip first (required for editable installs with pyproject.toml)
pip3 install --upgrade pip

# Install in editable mode (recommended for easy updates)
pip3 install -e .

# Or install normally
pip3 install .
```

> **Note:** Editable installs (`-e`) require pip 21.3 or newer. If you see an error about 
> `setup.py` not found, upgrade pip first with `pip3 install --upgrade pip`.

### Running the tool

After installation, you can run the tool in several ways:

```bash
# Option 1: Direct command (if ~/.local/bin is in your PATH)
data-diff --help

# Option 2: Run as a Python module (always works)
python3 -m data_diff_checker --help
```

If `data-diff` gives "command not found", either:
- Add `~/.local/bin` to your PATH: `export PATH="$HOME/.local/bin:$PATH"` (add to `~/.zshrc` or `~/.bashrc`)
- Or just use `python3 -m data_diff_checker` instead

### Updating

If you installed in editable mode (`pip3 install -e .`):
```bash
cd data-diff-checker
git pull
# Changes are immediately available - no reinstall needed!
```

If you installed normally (`pip3 install .`):
```bash
cd data-diff-checker
git pull
pip3 install .  # Reinstall after pulling
```

## Quick Start

### Compare two local CSV files

```bash
python3 -m data_diff_checker --local-prod production.csv --local-dev development.csv
```

### Use a composite primary key

```bash
python3 -m data_diff_checker --local-prod prod.csv --local-dev dev.csv --primary-key "sku,locale"
```

### Batch process a folder of file pairs

```bash
python3 -m data_diff_checker --local-folder ./responses/
```

### Quick test with row limit

```bash
python3 -m data_diff_checker --local-prod large_prod.csv --local-dev large_dev.csv --diff-rows 1000
```

## Usage

### Modes of Operation

#### 1. Local File Mode
Compare two CSV files directly:

```bash
python3 -m data_diff_checker --local-prod baseline.csv --local-dev compare.csv
```

#### 2. Folder Mode  
Batch process file pairs matching the pattern `prod_response_<N>_<hash>.txt` and `dev_response_<N>_<hash>.txt`:

```bash
python3 -m data_diff_checker --local-folder ./test_responses/
```

#### 3. URL Mode
Fetch and compare CSV responses from APIs:

```bash
python3 -m data_diff_checker --params-file test_cases.csv \
  --prod-url "https://api.prod.example.com/endpoint" \
  --dev-url "https://api.dev.example.com/endpoint"
```

The params file should be a CSV with a `params` column containing URL query strings:
```csv
params
"connection_info[shop_name]=store1&connection_info[api_key]=xxx"
"connection_info[shop_name]=store2&connection_info[api_key]=yyy"
```

### Command Line Options

```
Core Options:
  --primary-key, -k     Primary key column(s) for row matching (default: id)
  --timeout, -t         HTTP request timeout in seconds (default: 900)
  --max-examples, -m    Max example IDs in output (default: 10)
  --diff-rows, -r       Max rows to process per file (default: no limit)
  --source-limit, -l    Limit test cases from params file

Input Sources:
  --local-prod          Local production CSV file
  --local-dev           Local development CSV file
  --local-folder, -f    Folder with response file pairs
  --params-file, -p     CSV file with URL parameters (default: params.csv)

URL Mode:
  --prod-url            Base URL for production environment
  --dev-url             Base URL for development environment

Concurrency:
  --max-concurrent-fetches, -F   Max parallel URL fetches (default: 250)
  --max-concurrent-diffs, -c     Max parallel diff operations (default: 10)

Output:
  --output-dir, -o      Directory for response files (default: responses)
  --summary-dir, -s     Directory for JSON summaries (default: summaries)

Other:
  --verbose, -v         Enable verbose output
  --help, -h            Show help message
```

## Output Format

### Summary JSON Structure

```json
{
  "mode": "local",
  "prod_file": "production.csv",
  "dev_file": "development.csv",
  "rows_added": 5,
  "rows_removed": 2,
  "rows_updated": 150,
  "rows_updated_excluded_only": 30,
  "detailed_key_update_counts": {
    "title": 45,
    "price": 30,
    "description": 75
  },
  "example_ids": {
    "SKU-001": {"prod_line_num": 42, "dev_line_num": 43},
    "SKU-002": {"prod_line_num": 108, "dev_line_num": 110}
  },
  "common_keys": ["id", "title", "price", "availability"],
  "prod_only_keys": ["legacy_field"],
  "dev_only_keys": ["new_field"],
  "prod_row_count": 10000,
  "dev_row_count": 10003,
  "runtime_seconds": 2.34
}
```

### Key Metrics

| Field | Description |
|-------|-------------|
| `rows_added` | Rows present in dev but not in prod |
| `rows_removed` | Rows present in prod but not in dev |
| `rows_updated` | Rows with meaningful changes (excludes inventory/availability columns) |
| `rows_updated_excluded_only` | Rows where only excluded columns changed |
| `detailed_key_update_counts` | Per-column change counts |
| `example_ids` | Sample changed rows with line numbers for debugging |

### URL Mode Output Structure

URL mode creates timestamped run folders:

```
responses/
└── 20241126_143052_params_pk_sku-locale/
    ├── run_metadata.json          # Run configuration
    ├── summary.json               # Diff results
    ├── prod_response_0_abc123.txt # Response files
    ├── dev_response_0_abc123.txt
    └── ...

summaries/
├── diffs_summary_<timestamp>.json         # All results
├── diffs_summary_updates_<timestamp>.json # Only differences  
└── diffs_summary_errors_<timestamp>.json  # Only errors
```

## Memory Optimizations

Data Diff Checker is designed to handle large files efficiently:

- **Streaming I/O**: Rows are processed one at a time, never loading entire files
- **Hash-based comparison**: Stores MD5 hashes instead of full row data
- **Cached metadata**: Headers and row counts are computed once and cached
- **Two-pass algorithm**: Quick hash comparison first, detailed diff only for changes
- **Incremental GC**: Garbage collection between operations

## Python API

```python
from data_diff_checker import StreamingCSVReader, EfficientDiffer

# Read CSV with automatic format detection
reader = StreamingCSVReader("data.csv")
headers = reader.read_headers()
for row in reader.iterate_rows():
    print(row)

# Compare two files
differ = EfficientDiffer(primary_keys=["id"])
result = differ.compute_diff("prod.csv", "dev.csv")
print(f"Rows updated: {result['rows_updated']}")
```

## Configuration

### Excluded Columns

By default, columns containing these patterns are excluded from "meaningful change" detection:
- `inventory`
- `availability`
- `_fdx`

Changes to these columns are tracked separately in `rows_updated_excluded_only`.

### Customizing via Code

```python
from data_diff_checker import EfficientDiffer

differ = EfficientDiffer(
    primary_keys=["sku", "locale"],
    max_examples=20,
    max_rows=10000,  # Limit for testing
    excluded_patterns=["inventory", "stock", "qty"],
)
```

### Local Configuration File

Create a `.data-diff.json` file in your project directory to set defaults (this file is gitignored):

```json
{
  "prod_url": "https://api.prod.example.com/endpoint",
  "dev_url": "https://api.dev.example.com/endpoint",
  "dedup_keys": ["connection_info[store_hash]"]
}
```

With this config, you can run URL mode without specifying URLs every time:

```bash
python3 -m data_diff_checker --params-file test_cases.csv
```

The tool searches for `.data-diff.json` in the current directory and parent directories (up to your home directory).

## Development

### Setup

```bash
git clone https://github.com/snowthen-o7/data-diff-checker.git
cd data-diff-checker
pip3 install --upgrade pip
pip3 install -e ".[dev]"
```

### Run Tests

```bash
pytest
```

### Code Quality

```bash
# Format
black src tests

# Lint  
ruff check src tests

# Type check
mypy src
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Changelog

### 1.0.0 (2025)
- Initial public release
- Modular package structure
- Streaming CSV processing
- Hash-based diff algorithm
- Composite primary key support
- Parallel processing for URL mode
- Cross-platform terminal UI