from pyg_base import dictable, loop, add_, getargspec
from pyg_cell import cell, cell_runner# -*- coding: utf-8 -*-


def test_cell_runner_decorate():
    W = cell_runner(decorate = loop(dict), as_data = True)
    rs = dictable(a = [dict(x = 1, y = 2), 
                           3, 
                           dict(x = 3, y = 4)], 
                      b = [5, 6, dict(x = 1, y = 2)],
                      name = ['a is dict', 'neither dict', 'both dicts'])
    rs = rs(c = W(lambda a, b: cell(add_, a = a, b = b)))
    assert rs.c == [dict(x = 6, y = 7), 9, dict(x = 4, y = 6)]


def test_cell_runner_changes_signature():
    ADDS_DB_KEYS_TO_ARGSPEC= cell_runner(db = ['x', 'key'])(lambda a, b: a + b)
    assert getargspec(ADDS_DB_KEYS_TO_ARGSPEC).args == ['x', 'key', 'a', 'b']    
    REPLACES_SPEC_USING_ARGS = cell_runner(args = dict(key = 'item'), db = ['x', 'item'])(lambda a, b: a+b)
    assert sorted(getargspec(REPLACES_SPEC_USING_ARGS).args) == ['a', 'b', 'key', 'x']
