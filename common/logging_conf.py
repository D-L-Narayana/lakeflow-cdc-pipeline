import json
import logging
import sys
import time

_STD = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    """One JSON object per line so logs can be shipped to ELK / CloudWatch / Log Analytics unchanged."""

    def format(self, r):
        d = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(r.created)) + f".{int(r.msecs):03d}Z",
            "level": r.levelname,
            "logger": r.name,
            "event": r.getMessage(),
        }
        d.update({k: v for k, v in r.__dict__.items() if k not in _STD})
        if r.exc_info:
            d["exception"] = self.formatException(r.exc_info)
        return json.dumps(d, default=str)


def get_logger(name: str) -> logging.Logger:
    lg = logging.getLogger(name)
    if not lg.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(JsonFormatter())
        lg.addHandler(h)
        lg.setLevel(logging.INFO)
        lg.propagate = False
    return lg
