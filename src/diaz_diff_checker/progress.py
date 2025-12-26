"""
Terminal progress display with in-place updates.

Provides a visual progress display with:
- Progress bars for fetches and diffs
- Recent activity log
- Cross-platform support (Unix and Windows 10+)
- Graceful fallback for non-TTY environments
"""

import shutil
import sys
import threading
import time
import logging
from datetime import datetime
from typing import List, Optional


def enable_windows_ansi_support() -> bool:
    """
    Enable ANSI escape code support on Windows 10+.
    
    Returns:
        True if successful or not on Windows, False if it failed.
    """
    if sys.platform != 'win32':
        return True
    
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        
        # Get handle to stderr (STD_ERROR_HANDLE = -12)
        STD_ERROR_HANDLE = -12
        handle = kernel32.GetStdHandle(STD_ERROR_HANDLE)
        
        # Get current console mode
        mode = ctypes.c_ulong()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return False
        
        # Enable ENABLE_VIRTUAL_TERMINAL_PROCESSING (0x0004)
        ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
        new_mode = mode.value | ENABLE_VIRTUAL_TERMINAL_PROCESSING
        
        if not kernel32.SetConsoleMode(handle, new_mode):
            return False
        
        return True
    except Exception:
        return False


# Try to enable ANSI support on Windows at module load time
_WINDOWS_ANSI_ENABLED = enable_windows_ansi_support()


class ProgressDisplay:
    """
    Terminal progress display with progress bars and activity log.
    
    Features:
        - In-place updates (no scrolling)
        - Dual progress bars for fetches and diffs
        - Recent activity log
        - Error counter
        - Elapsed time tracking
        - Automatic fallback for non-TTY environments
    
    Example:
        >>> progress = ProgressDisplay(total_fetches=20, total_diffs=10)
        >>> progress.initial_draw()
        >>> progress.log("Starting process...")
        >>> progress.increment_fetches()
        >>> progress.finish()
    
    Args:
        total_fetches: Total number of fetch operations expected
        total_diffs: Total number of diff operations expected
        max_log_lines: Maximum number of log lines to display (default: 8)
    """
    
    def __init__(
        self, 
        total_fetches: int, 
        total_diffs: int, 
        max_log_lines: int = 8
    ):
        self.total_fetches = total_fetches
        self.total_diffs = total_diffs
        self.max_log_lines = max_log_lines
        
        # Progress counters
        self.completed_fetches = 0
        self.completed_diffs = 0
        self.errors = 0
        self.start_time = time.time()
        
        # Activity log
        self.log_lines: List[str] = []
        self.lock = threading.Lock()
        
        # Terminal capability detection
        self.is_windows = sys.platform == 'win32'
        if self.is_windows:
            # On Windows, only use TTY mode if ANSI support was successfully enabled
            self.is_tty = sys.stderr.isatty() and _WINDOWS_ANSI_ENABLED
        else:
            # On Unix-like systems, just check if it's a TTY
            self.is_tty = sys.stderr.isatty()
        
        self.display_height = 0  # Track how many lines we've drawn
        
        # Fallback mode: throttle progress logs
        self._last_progress_log: float = 0.0
        self._progress_log_interval: float = 5.0  # Log every 5 seconds in fallback mode
        
        # Background timer for elapsed time updates
        self._timer_thread: Optional[threading.Thread] = None
        self._timer_stop = threading.Event()
        
        # Get terminal width
        try:
            self.term_width = shutil.get_terminal_size().columns
        except Exception:
            self.term_width = 80
    
    def _timer_loop(self) -> None:
        """Background thread that updates display every second."""
        while not self._timer_stop.wait(timeout=1.0):
            with self.lock:
                if self.is_tty and not self._timer_stop.is_set():
                    self._draw()
    
    def _make_progress_bar(
        self, 
        current: int, 
        total: int, 
        width: int = 30, 
        label: str = ""
    ) -> str:
        """Create a progress bar string."""
        if total == 0:
            pct: float = 100.0
            filled = width
        else:
            pct = (current / total) * 100
            filled = int(width * current / total)
        
        bar = "█" * filled + "░" * (width - filled)
        return f"{label}[{bar}] {current}/{total} ({pct:.1f}%)"
    
    def _format_elapsed(self) -> str:
        """Format elapsed time as MM:SS."""
        elapsed = time.time() - self.start_time
        mins, secs = divmod(int(elapsed), 60)
        return f"{mins:02d}:{secs:02d}"
    
    def _clear_display(self) -> None:
        """Clear the previous display area using ANSI escape codes."""
        if self.display_height > 0:
            # Move cursor up and clear each line
            sys.stderr.write(f"\033[{self.display_height}A")  # Move up
            for _ in range(self.display_height):
                sys.stderr.write("\033[2K\n")  # Clear line
            sys.stderr.write(f"\033[{self.display_height}A")  # Move back up
    
    def _draw(self) -> None:
        """Draw the progress display (TTY mode only)."""
        if not self.is_tty:
            return
        
        lines = []
        
        # Header with elapsed time
        elapsed = self._format_elapsed()
        header_fill = "─" * max(0, self.term_width - 40)
        lines.append(f"┌─ Diaz Diff Checker ─ Elapsed: {elapsed} ─{header_fill}┐")
        
        # Progress bars
        fetch_bar = self._make_progress_bar(
            self.completed_fetches, self.total_fetches, 25, "Fetches: "
        )
        diff_bar = self._make_progress_bar(
            self.completed_diffs, self.total_diffs, 25, "Diffs:   "
        )
        
        content_width = self.term_width - 4
        lines.append(f"│ {fetch_bar:<{content_width}} │")
        lines.append(f"│ {diff_bar:<{content_width}} │")
        
        # Error count if any
        if self.errors > 0:
            error_text = f"⚠ Errors: {self.errors}"
            lines.append(f"│ {error_text:<{content_width}} │")
        
        # Separator
        sep_fill = "─" * max(0, self.term_width - 21)
        lines.append(f"├─ Recent Activity ─{sep_fill}┤")
        
        # Recent log lines
        recent_logs = self.log_lines[-self.max_log_lines:]
        for log in recent_logs:
            truncated = log[:content_width]
            lines.append(f"│ {truncated:<{content_width}} │")
        
        # Pad with empty lines
        for _ in range(self.max_log_lines - len(recent_logs)):
            lines.append(f"│ {'':<{content_width}} │")
        
        # Footer
        lines.append(f"└{'─' * (self.term_width - 2)}┘")
        
        # Clear previous and draw new
        self._clear_display()
        
        for line in lines:
            sys.stderr.write(line[:self.term_width] + "\n")
        
        sys.stderr.flush()
        self.display_height = len(lines)
    
    def _maybe_log_progress(self) -> None:
        """In non-TTY mode, periodically log progress to avoid spam."""
        if self.is_tty:
            return
        
        now = time.time()
        if now - self._last_progress_log >= self._progress_log_interval:
            self._last_progress_log = now
            elapsed = self._format_elapsed()
            
            fetch_pct = (
                (self.completed_fetches / self.total_fetches * 100) 
                if self.total_fetches > 0 else 100
            )
            diff_pct = (
                (self.completed_diffs / self.total_diffs * 100) 
                if self.total_diffs > 0 else 100
            )
            
            logging.info(
                f"Progress [{elapsed}]: "
                f"Fetches {self.completed_fetches}/{self.total_fetches} ({fetch_pct:.0f}%), "
                f"Diffs {self.completed_diffs}/{self.total_diffs} ({diff_pct:.0f}%)"
            )
    
    def log(self, message: str) -> None:
        """
        Add a log message and update display.
        
        In TTY mode, adds to the activity log in the display.
        In non-TTY mode, logs via standard logging.
        
        Args:
            message: The message to log
        """
        with self.lock:
            timestamp = datetime.now().strftime("%H:%M:%S")
            formatted = f"{timestamp} {message}"
            self.log_lines.append(formatted)
            
            # Keep only recent lines
            if len(self.log_lines) > 100:
                self.log_lines = self.log_lines[-100:]
            
            if self.is_tty:
                self._draw()
            else:
                logging.info(message)
    
    def update_fetches(self, completed: int) -> None:
        """Set the fetch progress to a specific value."""
        with self.lock:
            self.completed_fetches = completed
            if self.is_tty:
                self._draw()
            else:
                self._maybe_log_progress()
    
    def update_diffs(self, completed: int) -> None:
        """Set the diff progress to a specific value."""
        with self.lock:
            self.completed_diffs = completed
            if self.is_tty:
                self._draw()
            else:
                self._maybe_log_progress()
    
    def increment_fetches(self) -> None:
        """Increment fetch count by one."""
        with self.lock:
            self.completed_fetches += 1
            if self.is_tty:
                self._draw()
            else:
                self._maybe_log_progress()
    
    def increment_diffs(self) -> None:
        """Increment diff count by one."""
        with self.lock:
            self.completed_diffs += 1
            if self.is_tty:
                self._draw()
            else:
                self._maybe_log_progress()
    
    def increment_errors(self) -> None:
        """Increment error count by one."""
        with self.lock:
            self.errors += 1
            if self.is_tty:
                self._draw()
    
    def finish(self) -> None:
        """Clear display, stop timer, and prepare for normal output."""
        # Stop the timer thread
        self._timer_stop.set()
        if self._timer_thread is not None:
            self._timer_thread.join(timeout=2.0)
            self._timer_thread = None
        
        if self.is_tty:
            self._clear_display()
            sys.stderr.flush()
    
    def initial_draw(self) -> None:
        """Draw the initial progress display and start timer."""
        if self.is_tty:
            self._draw()
            # Start background timer for elapsed time updates
            self._timer_stop.clear()
            self._timer_thread = threading.Thread(target=self._timer_loop, daemon=True)
            self._timer_thread.start()