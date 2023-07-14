from pyg_cell._db_cell import db_cell, cell
from pyg_base import wrapper, argspec_update, ulist, as_list, Dict
from functools import partial



class cell_runner(wrapper):
    """
    a wrapper on functions that construct and return a cell
    
    Parameters:
    -----------
    go, load: int
        parameters in cell(go, load)

    db: None/str(s)/partial to table_cursor
        database. This is a special variable as we will add the     
    
    args: str/list of str/dict of str:str
        dict of keys needed in the function signature and their relabeled value on how they are to be fed into the function
    
    defaults: dict of the above args with optional values
    
    constants: dict
        values to update the cell without adding to function signature
    
    example:
    --------
    
    >>> W = cell_runner(args = ['key'], db = ['x', 'key'])
    >>> rs = dictable(x = [1,2,3,4])
    >>> rs = rs(x2 = W(lambda x: db_cell(add_, a = x, b = x)))

    >>> assert cell_item(rs.x2) == [2,4,6,8]
    >>> assert rs[lambda x2: x2.key] == ['x2'] * 4
    >>> assert rs[lambda x2: x2.x] == rs.x

    Example: changing function signature
    --------
    >>> W = cell_runner(db = ['x', 'key'])(lambda a, b: a + b)
    >>> assert getargspec(W).args == ['x', 'key', 'a', 'b']
    
    >>> W = cell_runner(args = dict(key = 'item'), db = ['x', 'item'])(lambda a, b: a+b)
    >>> assert sorted(getargspec(W).args) == ['a', 'b', 'key', 'x']

    
    Example: changing function signature with added defaults
    --------
    >>> W = cell_runner(args = dict(key = 'item'), db = ['x', 'item'], defaults = dict(key = 'james'))(lambda a, b: a+b)
    >>> assert getargspec(W).args == ['x', 'a', 'b', 'key']
    >>> assert getargspec(W).defaults == ('james', )
    >>> W = cell_runner(args = dict(key = 'item'), db = ['x', 'item'], defaults = dict(key = 'james'))(lambda a, b = 1: a+b)
    >>> assert getargspec(W).defaults == (1, 'james')
    """
    
    def __init__(self, function = None, go = 0, load = 0, db = None, args = None, defaults = None, constants = None):
        defaults_ = defaults or {}
        args_ = {key : key for key in defaults_}
        if not args:
            pass
        elif isinstance(args, str):
            args_.update({args: args})
        elif isinstance(args, list):
            args_.update(dict(zip(args, args)))
        elif isinstance(args, dict):
            args_.update(args)
        else:
            raise ValueError(f'not sure what args are {args}')
        if isinstance(db, str) and '%' in db:
            db_ = [key.split('.')[0].split('/')[0].split('\\')[0] for key in db.split('%')[1:]]
            args_.update(dict(zip(db_, db_)))
        elif isinstance(db, (str,list)):
            db_ = [v for v in as_list(db) if v not in args_.values()]
            args_.update(dict(zip(db_, db_)))
        elif isinstance(db, partial):
            db_ = [v for v in as_list(db.keywords.get('pk')) if v not in args_.values()]
            args_.update(dict(zip(db_, db_)))
        return super(cell_runner, self).__init__(function = function, go = go, load = load, db = db, args = args_, defaults = defaults_, constants = constants or {})

    def wrapped(self, **kwargs):
        kws = Dict(self.constants)
        kws.update(self.defaults)
        kws.update(kwargs)
        kws = kws.relabel(self.args)
        if isinstance(self.function, cell):
            res = self.function.copy()
        else:
            res = kws.apply(self.function)
        if isinstance(res, cell):
            res.update(kws - res.keys())
            if self.db and res.get('db') is None:
                res.db = self.db
            res = res(self.go, self.load)
        return res

    @property
    def required(self):
        """
        required variables and what they are relabeled to
        """
        return ulist(self.args.keys()) - list(self.defaults.keys())
    
    @property
    def fullargspec(self):
        """
        updates the function so that its signature will require the required and/or optional keys

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        spec = super(cell_runner, self).fullargspec
        args = self.required + spec.args
        if self.defaults:
            optional = {k : v for k, v in self.defaults.items() if k not in args}
            args = args + list(optional.keys())
            defaults = (spec.defaults or ()) + tuple(optional.values())
            return argspec_update(spec, args = args, defaults = defaults)
        else:
            return argspec_update(spec, args = args)
            

