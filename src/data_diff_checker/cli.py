"""
Command-line interface for Data Diff Checker.

Provides argument parsing and CLI entry point.
"""

import argparse
from .config import (
    DEFAULT_MAX_EXAMPLES,
    DEFAULT_TIMEOUT,
    DEFAULT_MAX_CONCURRENT_FETCHES,
    DEFAULT_MAX_CONCURRENT_DIFFS,
    DEFAULT_PRIMARY_KEY,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SUMMARY_DIR,
    get_config_value,
)


BANNER = r"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║   ██████╗ ██╗ █████╗ ███████╗    ██████╗ ██╗███████╗███████╗                 ║
║   ██╔══██╗██║██╔══██╗╚══███╔╝    ██╔══██╗██║██╔════╝██╔════╝                 ║
║   ██║  ██║██║███████║  ███╔╝     ██║  ██║██║█████╗  █████╗                   ║
║   ██║  ██║██║██╔══██║ ███╔╝      ██║  ██║██║██╔══╝  ██╔══╝                   ║
║   ██████╔╝██║██║  ██║███████╗    ██████╔╝██║██║     ██║                      ║
║   ╚═════╝ ╚═╝╚═╝  ╚═╝╚══════╝    ╚═════╝ ╚═╝╚═╝     ╚═╝                      ║
║                                                                              ║
║            ██████╗██╗  ██╗███████╗ ██████╗██╗  ██╗███████╗██████╗            ║
║           ██╔════╝██║  ██║██╔════╝██╔════╝██║ ██╔╝██╔════╝██╔══██╗           ║
║           ██║     ███████║█████╗  ██║     █████╔╝ █████╗  ██████╔╝           ║
║           ██║     ██╔══██║██╔══╝  ██║     ██╔═██╗ ██╔══╝  ██╔══██╗           ║
║           ╚██████╗██║  ██║███████╗╚██████╗██║  ██╗███████╗██║  ██║           ║
║            ╚═════╝╚═╝  ╚═╝╚══════╝ ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝           ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""


class CustomHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """Custom formatter for prettier help output."""
    
    def __init__(self, prog, indent_increment=2, max_help_position=40, width=100):
        super().__init__(prog, indent_increment, max_help_position, width)
    
    def _format_action_invocation(self, action):
        if not action.option_strings:
            return super()._format_action_invocation(action)
        parts = []
        if action.option_strings:
            parts.append(', '.join(action.option_strings))
        return '  '.join(parts)


def create_parser() -> argparse.ArgumentParser:
    """
    Create and configure the argument parser.
    
    Returns:
        Configured ArgumentParser instance
    """
    description = f"""{BANNER}
  Memory-optimized CSV diff tool with streaming processing.
  
  Compare CSV responses between production and development environments,
  with support for local file comparison, folder-based batch processing,
  and URL-based automated testing.

┌─────────────────────────────────────────────────────────────────────────────┐
│  MODES OF OPERATION                                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. URL Mode (default)                                                      │
│     Fetches responses from prod/dev URLs and compares them.                 │
│     Requires: --params-file with URL parameters                             │
│     Output: Creates timestamped run folder with all responses               │
│                                                                             │
│  2. Local File Mode                                                         │
│     Compares two local CSV files directly.                                  │
│     Requires: --local-prod and --local-dev                                  │
│                                                                             │
│  3. Folder Mode                                                             │
│     Batch processes all prod/dev file pairs in a folder.                    │
│     Requires: --local-folder                                                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
"""
    
    epilog = """
┌─────────────────────────────────────────────────────────────────────────────┐
│  EXAMPLES                                                                   │
└─────────────────────────────────────────────────────────────────────────────┘

  Compare local files:
  ─────────────────────
    %(prog)s --local-prod production.csv --local-dev development.csv

  Compare with composite primary key:
  ────────────────────────────────────
    %(prog)s --local-prod prod.csv --local-dev dev.csv --primary-key "sku,locale"

  Run URL-based tests with custom timeout:
  ─────────────────────────────────────────
    %(prog)s --params-file test_params.csv --timeout 1200 --verbose

  Batch process a folder of response files:
  ──────────────────────────────────────────
    %(prog)s --local-folder ./responses --primary-key id

  Test with only first 10 URLs from params file:
  ───────────────────────────────────────────────
    %(prog)s --params-file all_tests.csv --source-limit 10

┌─────────────────────────────────────────────────────────────────────────────┐
│  OUTPUT STRUCTURE                                                           │
└─────────────────────────────────────────────────────────────────────────────┘

  Response files are saved to timestamped folders:
  
    responses/
    └── 20241126_143052_params_pk_sku-locale/
        ├── run_metadata.json
        ├── summary.json
        ├── prod_response_0_abc123.txt
        ├── dev_response_0_abc123.txt
        └── ...

  Summary files are written to the summary directory:
  
    summaries/
    ├── diffs_summary_<timestamp>.json         # All results
    ├── diffs_summary_updates_<timestamp>.json # Only differences
    └── diffs_summary_errors_<timestamp>.json  # Only errors

┌─────────────────────────────────────────────────────────────────────────────┐
│  MEMORY OPTIMIZATIONS                                                       │
└─────────────────────────────────────────────────────────────────────────────┘

  • True streaming CSV processing (no full file loading)
  • Hash-based row comparison (stores hashes, not full row data)
  • Cached headers and row counts (avoids redundant file reads)
  • Incremental garbage collection between test cases
  • Two-pass algorithm: quick hash comparison, then detailed diff

┌─────────────────────────────────────────────────────────────────────────────┐
│  NOTES                                                                      │
└─────────────────────────────────────────────────────────────────────────────┘

  • Auto-detects CSV delimiters (comma or tab)
  • Gzipped responses are automatically decompressed
  • rows_updated counts only meaningful changes (excludes inventory/availability)
  • Example IDs only include rows with meaningful changes
"""
    
    parser = argparse.ArgumentParser(
        prog='data-diff',
        description=description,
        epilog=epilog,
        formatter_class=CustomHelpFormatter,
        add_help=False,
    )
    
    # Help group
    help_group = parser.add_argument_group(
        '📖 Help',
        'Display help information'
    )
    help_group.add_argument(
        '-h', '--help',
        action='help',
        default=argparse.SUPPRESS,
        help='Show this help message and exit'
    )
    
    # Core options
    core_group = parser.add_argument_group(
        '⚙️  Core Options',
        'Primary configuration for diff operations'
    )
    core_group.add_argument(
        '--primary-key', '-k',
        type=str,
        default=DEFAULT_PRIMARY_KEY,
        metavar='KEY',
        help=f'Primary key column(s) for row matching.\n'
             f'Use comma-separated values for composite keys.\n'
             f'Example: "id" or "sku,locale"\n'
             f'(default: {DEFAULT_PRIMARY_KEY})'
    )
    core_group.add_argument(
        '--timeout', '-t',
        type=int,
        default=DEFAULT_TIMEOUT,
        metavar='SECS',
        help=f'HTTP request timeout in seconds.\n'
             f'(default: {DEFAULT_TIMEOUT} = 15 minutes)'
    )
    core_group.add_argument(
        '--max-examples', '-m',
        type=int,
        default=DEFAULT_MAX_EXAMPLES,
        metavar='NUM',
        help=f'Maximum number of example IDs to include\n'
             f'in output for tracking row differences.\n'
             f'(default: {DEFAULT_MAX_EXAMPLES})'
    )
    core_group.add_argument(
        '--max-concurrent-diffs', '-c',
        type=int,
        default=DEFAULT_MAX_CONCURRENT_DIFFS,
        metavar='NUM',
        help=f'Maximum number of diffs to run in parallel.\n'
             f'(default: {DEFAULT_MAX_CONCURRENT_DIFFS})'
    )
    core_group.add_argument(
        '--max-concurrent-fetches', '-F',
        type=int,
        default=DEFAULT_MAX_CONCURRENT_FETCHES,
        metavar='NUM',
        help=f'Maximum concurrent URL fetch operations.\n'
             f'(default: {DEFAULT_MAX_CONCURRENT_FETCHES})'
    )
    core_group.add_argument(
        '--diff-rows', '-r',
        type=int,
        default=None,
        metavar='NUM',
        dest='diff_rows',
        help='Maximum rows to process per CSV file.\n'
             'Useful for quick testing on large files.\n'
             '(default: no limit)'
    )
    core_group.add_argument(
        '--source-limit', '-l',
        type=int,
        default=None,
        metavar='NUM',
        dest='source_limit',
        help='Limit number of test cases from params file.\n'
             'Useful for quick testing with subset of URLs.\n'
             '(default: no limit, process all)'
    )
    
    # Input sources
    input_group = parser.add_argument_group(
        '📥 Input Sources',
        'Specify input files or folders (choose one mode)'
    )
    input_group.add_argument(
        '--params-file', '-p',
        type=str,
        default='params.csv',
        metavar='FILE',
        help='CSV file containing URL parameters.\n'
             'Must have a "params" column.\n'
             '(default: params.csv)'
    )
    input_group.add_argument(
        '--local-prod',
        type=str,
        default='',
        metavar='FILE',
        help='Local production CSV file to compare.\n'
             'Use with --local-dev for local mode.'
    )
    input_group.add_argument(
        '--local-dev',
        type=str,
        default='',
        metavar='FILE',
        help='Local development CSV file to compare.\n'
             'Use with --local-prod for local mode.'
    )
    input_group.add_argument(
        '--local-folder', '-f',
        type=str,
        default='',
        metavar='DIR',
        help='Folder containing response file pairs.\n'
             'Files must match pattern:\n'
             '  prod_response_<N>_<hash>.txt\n'
             '  dev_response_<N>_<hash>.txt'
    )
    
    # URL mode configuration
    url_group = parser.add_argument_group(
        '🌐 URL Mode Configuration',
        'Settings for URL fetch mode'
    )
    
    # Get defaults from config file
    default_prod_url = get_config_value('prod_url', '')
    default_dev_url = get_config_value('dev_url', '')
    
    prod_url_help = 'Base URL for production environment.\nParameters from params file are appended.'
    if default_prod_url:
        prod_url_help += f'\n(from config: {default_prod_url[:50]}...)'
    
    dev_url_help = 'Base URL for development environment.\nParameters from params file are appended.'
    if default_dev_url:
        dev_url_help += f'\n(from config: {default_dev_url[:50]}...)'
    
    url_group.add_argument(
        '--prod-url',
        type=str,
        default=default_prod_url,
        metavar='URL',
        help=prod_url_help
    )
    url_group.add_argument(
        '--dev-url',
        type=str,
        default=default_dev_url,
        metavar='URL',
        help=dev_url_help
    )

    default_jwt = get_config_value('jwt', '')
    jwt_help = (
        'JWT token sent as "Authorization: Bearer <token>"\n'
        'on every prod/dev fetch.'
    )
    if default_jwt:
        jwt_help += '\n(from config: ***)'
    url_group.add_argument(
        '--jwt',
        type=str,
        default=default_jwt,
        metavar='TOKEN',
        help=jwt_help,
    )
    url_group.add_argument(
        '--header', '-H',
        action='append',
        default=[],
        metavar='KEY=VALUE',
        dest='headers',
        help='Additional HTTP header to send on every fetch.\n'
             'Repeatable. Overrides --jwt if KEY is Authorization.\n'
             'Example: -H "X-Api-Key=abc123"',
    )
    
    # Output configuration
    output_group = parser.add_argument_group(
        '📤 Output Configuration',
        'Control where results are saved'
    )
    output_group.add_argument(
        '--output-dir', '-o',
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        metavar='DIR',
        help=f'Base directory for response files.\n'
             f'A timestamped subfolder is created per run.\n'
             f'(default: {DEFAULT_OUTPUT_DIR})'
    )
    output_group.add_argument(
        '--summary-dir', '-s',
        type=str,
        default=DEFAULT_SUMMARY_DIR,
        metavar='DIR',
        help=f'Directory for JSON summary reports.\n'
             f'(default: {DEFAULT_SUMMARY_DIR})'
    )
    
    # Debugging
    debug_group = parser.add_argument_group(
        '🔍 Debugging',
        'Options for troubleshooting and verbose output'
    )
    debug_group.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose/debug output.\n'
             'Shows detailed progress and timing info.'
    )
    
    return parser


def main():
    """Main entry point for the CLI."""
    from .main import run_main
    
    parser = create_parser()
    args = parser.parse_args()
    run_main(args)


if __name__ == "__main__":
    main()