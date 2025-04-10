# --------------------------------------------------------------------------------------------------
# This file contains the required functions to log messages in different levels and manage those logs.
# --------------------------------------------------------------------------------------------------

from datetime import datetime
from typing import Union


class LogLevel:
    """Enumeration for log levels.

    This enumeration defines the various log levels used in the logging system.
    Each log level represents a different severity of messages that can be logged.

    - Trace: Used for very detailed logging, typically for diagnosing issues.
    - Debug: Used for general debugging information.
    - Info: Used for informational messages that highlight the progress of the application.
    - Success: Used to indicate successful operations.
    - Warn: Used to indicate potentially harmful situations.
    - Error: Used to indicate error events that might still allow the application to continue running.
    - Fatal: Used to indicate very severe error events that will presumably lead the application to abort.
    """

    Trace = 1
    Debug = 2
    Info = 3
    Success = 4
    Warn = 5
    Error = 6
    Fatal = 7


LogLevelType = Union[
    LogLevel.Trace,
    LogLevel.Debug,
    LogLevel.Info,
    LogLevel.Success,
    LogLevel.Warn,
    LogLevel.Error,
    LogLevel.Fatal,
]
"""LogLevelType represents the various log levels defined in the LogLevel enumeration.
It exists to provide type safety and clarity in the logging functions, ensuring that only valid log levels are used when logging messages. This helps prevent errors and improves code readability by explicitly defining the expected log levels."""

log_level: LogLevelType = LogLevel.Info
"""
log_level represents the current logging threshold for the application.
Messages with log level lower than this threshold will not be logged.
"""


class LogFormat:
    """Class for log formatting constants."""

    start = "\033["
    start_end = "m"
    end = "\033[0m"
    bold = ";1"
    underline = ";4"
    purple = ";95"
    cyan = ";96"
    darkcyan = ";36"
    light_gray = ";37"
    blue = ";34"
    green = ";92"
    yellow = ";93"
    red = ";91"


def get_log_level_str(level: LogLevelType) -> str:
    """Returns the string representation of the log level."""
    return {
        LogLevel.Trace: "TRACE  ",
        LogLevel.Debug: "DEBUG  ",
        LogLevel.Info: "INFO   ",
        LogLevel.Success: "SUCCESS",
        LogLevel.Warn: "WARN   ",
        LogLevel.Error: "ERROR  ",
        LogLevel.Fatal: "FATAL  ",
    }.get(level, "UNKNOWN")


def get_log_level_color(level: LogLevelType) -> str:
    """Returns the color code for the log level."""
    return {
        LogLevel.Trace: LogFormat.light_gray,
        LogLevel.Debug: LogFormat.light_gray,
        LogLevel.Info: LogFormat.blue,
        LogLevel.Success: LogFormat.green,
        LogLevel.Warn: LogFormat.yellow,
        LogLevel.Error: LogFormat.red,
        LogLevel.Fatal: LogFormat.red,
    }.get(level, LogFormat.light_gray)


def log(level: LogLevelType, msg: str):
    """Logs a message with the specified log level."""

    if level < log_level:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    level_str = get_log_level_str(level)
    color = get_log_level_color(level)
    formatted_msg = f"{LogFormat.start}{LogFormat.bold}{color}{LogFormat.start_end}{timestamp} - {level_str}:{LogFormat.end} {msg}"

    print(formatted_msg)


def log_trace(msg: str):
    """Logs a trace message."""
    log(level=LogLevel.Trace, msg=msg)


def log_debug(msg: str):
    """Logs a debug message."""
    log(level=LogLevel.Debug, msg=msg)


def log_info(msg: str):
    """Logs an info message."""
    log(level=LogLevel.Info, msg=msg)


def log_success(msg: str):
    """Logs a success message."""
    log(level=LogLevel.Success, msg=msg)


def log_warn(msg: str):
    """Logs a warning message."""
    log(level=LogLevel.Warn, msg=msg)


def log_error(msg: str):
    """Logs an error message."""
    log(level=LogLevel.Error, msg=msg)


def log_fatal(msg: str):
    """Logs a fatal message."""
    log(level=LogLevel.Fatal, msg=msg)
