import logging
import os
import sys

__all__ = ["hbi_root_logger", "get_logger"]

hbi_root_logger = None


def get_logger(name: str, root_name="hbi"):
    global hbi_root_logger

    assert root_name is not None, "null root name is not acceptable!"
    if hbi_root_logger is None:
        hbi_root_logger = logging.getLogger(root_name)
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter(
                "[%(asctime)s %(process)d](%(name)s) %(message)s\n"
                ' -%(levelname)s- File "%(pathname)s", line %(lineno)d, in %(funcName)s',
                "%FT%T",
            )
        )
        hbi_root_logger.handlers.append(handler)
        log_level = logging.INFO
        log_level_name = os.environ.get("HBI_LOG_LEVEL", "INFO")
        try:
            log_level = getattr(logging, log_level_name.upper())
        except AttributeError:
            hbi_root_logger.error(f"Failed setting log level to [{log_level_name}]")
        hbi_root_logger.setLevel(log_level)

    if name is None or name == "":
        name = root_name
    elif not name.startswith(f"{root_name}."):
        raise ValueError(f"Root logger [{root_name}] is not parent of [{name}] !")

    return logging.getLogger(name)


hbi_root_logger = get_logger(None)
