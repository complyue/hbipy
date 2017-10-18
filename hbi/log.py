import logging
import os
import sys

__all__ = [
    'hbi_root_logger'
]

hbi_root_logger = logging.getLogger('hbi')

if len(hbi_root_logger.handlers) <= 0:
    # This means logging is not configured otherwise for HBI, so we can do our defaults

    hbi_root_logger.handlers.append(
        logging.StreamHandler(sys.stderr)
    )
    log_level = logging.INFO
    log_level_name = os.environ.get('HBI_LOG_LEVEL', 'INFO')
    try:
        log_level = getattr(logging, log_level_name.upper())
    except AttributeError:
        hbi_root_logger.error(f'HBI failed setting log level to [{log_level_name}]')
    hbi_root_logger.setLevel(log_level)
