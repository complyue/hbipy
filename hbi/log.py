import logging
import os
import sys

__all__ = [
    'hbi_root_logger', 'get_logger',
]


def get_logger(name: str, root_name=None):
    if root_name is None:
        root_name = name.split('.')[0]
    elif not name.startswith(f'{root_name}.'):
        raise ValueError(f'Root logger [{root_name}] is not parent of [{name}] !')

    root_logger = logging.getLogger(root_name)

    if len(root_logger.handlers) <= 0:
        # This means logging is not configured otherwise, so we can do the defaults
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter(
            "[%(asctime)s %(process)d](%(name)s) %(message)s\n"
            ' -%(levelname)s- File "%(pathname)s", line %(lineno)d, in %(funcName)s',
            "%FT%T"
        ))
        root_logger.handlers.append(handler)
        log_level = logging.INFO
        log_level_name = os.environ.get('HBI_LOG_LEVEL', 'INFO')
        try:
            log_level = getattr(logging, log_level_name.upper())
        except AttributeError:
            root_logger.error(f'Failed setting log level to [{log_level_name}]')
        root_logger.setLevel(log_level)

    return logging.getLogger(name)


hbi_root_logger = get_logger('hbi')
