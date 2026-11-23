import logging
import sys


def _format_value(value):
    if isinstance(value, float):
        return f"{value:.2f}"
    text = str(value)
    if text == "" or any(ch in text for ch in " =\""):
        return '"' + text.replace('"', '\\"') + '"'
    return text


class KeyValueFormatter(logging.Formatter):
    def format(self, record):
        timestamp = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        line = (
            f"{timestamp}.{int(record.msecs):03d} {record.levelname:<7} "
            f"[{record.threadName}] {record.name} event={record.getMessage()}"
        )
        fields = getattr(record, "fields", None)
        if fields:
            line += " " + " ".join(f"{key}={_format_value(value)}" for key, value in fields.items())
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


def setup_logging(name, level=logging.INFO):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(KeyValueFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    return logging.getLogger(name)


def log_event(logger, event, level=logging.INFO, **fields):
    logger.log(level, event, extra={"fields": fields})


class ProgressLogger:
    def __init__(self, logger, event, total, step_percent=25, **fields):
        self.logger = logger
        self.event = event
        self.total = total
        self.step = step_percent
        self.next_mark = step_percent
        self.fields = fields

    def update(self, done):
        if self.total <= 0:
            return
        percent = done * 100 // self.total
        while self.next_mark < 100 and percent >= self.next_mark:
            log_event(
                self.logger, self.event, logging.INFO,
                **self.fields, percent=self.next_mark, bytes=done, total=self.total,
            )
            self.next_mark += self.step
