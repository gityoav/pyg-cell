from pyg_base import dictable, loop, add_, getargspec, pd2np, drange,eq , Dict
from pyg_cell import cell, cell_runner, cell_func, cell_go, cell_item
from numba import jit
import pandas as pd
import numpy as np


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



@pd2np
@jit
def fnna(a):
    for i in range(a.shape[0]):
        if ~np.isnan(a[i]):
            return i


@loop(pd.DataFrame)
@pd2np
def df_fnna_to_value(a, new_value = 0):
    res = a.copy()
    res[:fnna(res)] = new_value
    return res

def test_cell_runner_with_double_loop():
    a = pd.DataFrame(dict(a = [np.nan, 1, np.nan, 3.]), drange(-3))
    b = pd.DataFrame(dict(a = [0.0, 1, np.nan, 3.]), drange(-3))
    assert fnna(a) == 1
    assert eq(df_fnna_to_value(a), b)
    row = Dict(a = a)
    row = row(b = cell_runner(lambda a: cell(df_fnna_to_value, a = a)))
    assert eq(row.b.data,b)

    
    