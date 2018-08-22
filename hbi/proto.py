__all__ = [
    'PACK_HEADER_MAX', 'PACK_BEGIN', 'PACK_LEN_END', 'PACK_END',
]

# max scanned length of packet header
PACK_HEADER_MAX = 60

PACK_BEGIN = b'['
PACK_LEN_END = b'#'
PACK_END = b']'
