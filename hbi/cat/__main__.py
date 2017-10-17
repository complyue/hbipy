"""

"""

assert '__hbi_serving__' != __name__, 'running hbi.cat as HBI server ?! not supposedly right.'


def __hbi_land__(code, wire_dir):
    print(rf'''
[#{wire_dir}]{code}
''', flush=True)


def hbi_disconnected():
    import sys
    print('HBI connection closed by peer.', file=sys.stderr)
    sys.exit(0)
