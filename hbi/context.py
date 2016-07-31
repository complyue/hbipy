"""
Run arbitrary Python code in supplied context and return evaluated value of last statement.

"""

import ast


def run_in_context(code, context, defs={}):
    ast_ = ast.parse(code, '<code>', 'exec')
    last_expr = None
    for field_ in ast.iter_fields(ast_):
        if 'body' != field_[0]: continue
        if len(field_[1]) > 0 and isinstance(field_[1][-1], ast.Expr):
            last_expr = ast.Expression()
            last_expr.body = field_[1].pop().value
    exec(compile(ast_, '<code>', 'exec'), context, defs)
    if last_expr:
        return eval(compile(last_expr, '<code>', 'eval'), context, defs)
    return None
