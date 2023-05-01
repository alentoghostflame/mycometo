import logging
import sys


__all__ = (
    "setup_logging",
)


LOGGING_FORMAT = "[{asctime}][{processName}][{filename}][{lineno:3}][{levelname}] {message}"
# LOGGING_LEVEL = logging.DEBUG


class ColoredFormatter(logging.Formatter):
    BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, LIGHT_GRAY = (f"\033[{i}m" for i in range(30, 38))
    GRAY, LIGHT_RED, LIGHT_GREEN, LIGHT_YELLOW, LIGHT_BLUE, LIGHT_MAGENTA, LIGHT_CYAN, WHITE = (
        f"\033[{i}m" for i in range(90, 98)
    )
    RESET_SEQ = "\033[0m"
    BLINKING_SEQ = "\033[5m"
    BOLD_SEQ = "\033[1m"
    LEVEL_COLORS = {
        'DEBUG': GRAY, 'INFO': CYAN, 'WARNING': LIGHT_YELLOW, 'ERROR': LIGHT_RED, 'CRITICAL': BLINKING_SEQ + RED,
    }
    TEXT_COLORS = {
        'DEBUG': GRAY, 'INFO': WHITE, 'WARNING': LIGHT_YELLOW, 'ERROR': LIGHT_RED, 'CRITICAL': RED,
    }

    def __init__(self):
        logging.Formatter.__init__(self, fmt=LOGGING_FORMAT, style="{")

    def format(self, record: logging.LogRecord):
        levelname = record.levelname
        if levelname in self.LEVEL_COLORS:
            levelname_color = self.LEVEL_COLORS[levelname] + levelname + self.RESET_SEQ
            record.levelname = levelname_color
        if levelname in self.TEXT_COLORS:
            template = self.TEXT_COLORS[levelname] + "{}" + self.RESET_SEQ
            record.msg = template.format(record.msg)
        return logging.Formatter.format(self, record)


log_format = ColoredFormatter()
log_console_handler = logging.StreamHandler(sys.stdout)
log_console_handler.setFormatter(log_format)


def setup_logging(logger_name, level=logging.DEBUG):
    setup_logger = logging.getLogger(logger_name)
    setup_logger.addHandler(log_console_handler)
    setup_logger.setLevel(level)
