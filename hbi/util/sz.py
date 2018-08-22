import re

__all__ = [
    'hrdsz', 'hwdsz',
]


def hrdsz(sz):
    """
    Human readable data size.

    """
    if sz is None or not isinstance(sz, (int, float)):
        # let None and non-numeric value pass through
        return sz

    # compare with 999* to avoid scientific notations from appearing

    if sz > 999 * 1024 ** 3:
        return f'{sz/(1024**4):0.3g}TB'
    if sz > 999 * 1024 ** 2:
        return f'{sz/(1024**3):0.3g}GB'
    if sz > 999 * 1024:
        return f'{sz/(1024**2):0.3g}MB'
    if sz > 999:
        return f'{sz/1024:0.3g}KB'

    return f'{sz}Byte'


def hwdsz(sz):
    """
    Human written data size.

    """

    if sz is None or isinstance(sz, int):
        # let None/integer pass through
        return sz

    # expect str representation of number, optionally plus a known unit
    sz = str(sz)
    m = re.match((
        r'\s*([0-9]+(\.[0-9]*)?)'  # numeric part, group 1 and 2
        r'(\s*(BYTES?|Bytes?|bytes?|B|b|KB|kb|K|k|MB|mb|M|m|GB|gb|G|g|TB|tb|T|t)\s*)?'  # unit part, group 3 and 4
    ), sz)
    if m is None:
        raise TypeError(f'Invalid data size: {sz!r}')
    if m.end() != len(sz):
        raise ValueError(f'Tailing part [{sz[m.end():]!r}] of [{sz!r}] not understood.')
    num_str, frac_part, unit_part, unit_name = m.groups()

    if unit_name is None:
        # unit-less, assuming byte
        if frac_part is None:
            return int(num_str)
        raise ValueError(f'Bytes can not be fractional!')

    unit_tk = unit_name[0].upper()
    if 'B' == unit_tk:
        # byte
        if frac_part is None:
            return int(num_str)
        raise ValueError(f'Bytes can not be fractional!')

    if 'K' == unit_tk:
        # kilobyte
        if frac_part is None:
            return 1024 * int(num_str)
        return round(1024 * float(num_str))

    if 'M' == unit_tk:
        # megabyte
        if frac_part is None:
            return 1024 ** 2 * int(num_str)
        return round(1024 ** 2 * float(num_str))

    if 'G' == unit_tk:
        # gigabyte
        if frac_part is None:
            return 1024 ** 3 * int(num_str)
        return round(1024 ** 3 * float(num_str))

    if 'T' == unit_tk:
        # terabyte
        if frac_part is None:
            return 1024 ** 4 * int(num_str)
        return round(1024 ** 4 * float(num_str))

    raise ValueError(f'Invalid size string: {sz!r}')
