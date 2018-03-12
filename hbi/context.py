"""
Run arbitrary Python code in supplied context and return evaluated value of last statement.

"""

import ast

__all__ = [
    'run_in_context',
]


def run_in_context(code, context, defs={}, src_name='<hbi-code>'):
    ast_ = ast.parse(code, src_name, 'exec')
    last_expr = None
    last_def_name = None
    for field_ in ast.iter_fields(ast_):
        if 'body' != field_[0]:
            continue
        if len(field_[1]) > 0:
            le = field_[1][-1]
            if isinstance(le, ast.Expr):
                last_expr = ast.Expression()
                last_expr.body = field_[1].pop().value
            elif isinstance(le, (ast.FunctionDef, ast.ClassDef)):
                last_def_name = le.name
    exec(compile(ast_, src_name, 'exec'), context, defs)
    if last_expr is not None:
        return eval(compile(last_expr, src_name, 'eval'), context, defs)
    elif last_def_name is not None:
        return defs[last_def_name]
    return None
