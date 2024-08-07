from pyg_base import as_list, is_primitive, get_cache, dictattr, Dict, tree_getitem, getargspec, getargs, getcallargs, \
                    eq, loop, ulist, tree_repr, wrapper, logger, is_strs, is_date, is_num, list_instances, is_ts, dictable, dictattr
                    
from pyg_cell._dag import get_DAG, get_GAD, add_edge, del_edge, topological_sort 
from pyg_encoders import as_writer, npy_write, pd_read_root
from copy import copy
import datetime
from functools import partial

_data = 'data'
_output = 'output'
_updated = 'updated'
_latest = 'latest'
_function = 'function'
_bind = 'bind'
_pk = 'pk'
_db = 'db'
_id = '_id'


__all__ = ['cell', 'cell_item', 'cell_go', 'cell_load', 'cell_func', 'cell_output', \
           'cell_clear', ]

# GRAPH = {}
# _GAD = {} ## a dict from child to parents, hence GAD as opposed to DAG

GRAPH = get_cache('GRAPH')
UPDATED = get_cache('UPDATED')


def cell_output(c): 
    """
    returns the keys the cell output is stored at. equivalent to cell._output
    """
    res = c.get(_output)
    if res is None or res is False:
        res = getattr(c.get(_function), _output, None)
    if res is None:
        res = _data
    return as_list(res)



@loop(list, tuple)
def _cell_item(value, key = None, key_before_data = False):
    if not isinstance(value, cell):
        if isinstance(value, dict):
            try:
                return type(value)(**{k: cell_item(v, key) for k, v in value.items()})
            except TypeError:
                if isinstance(value, dictable):
                    return type(value)(**{str(k): cell_item(v, key) for k, v in value.items()})
                else:
                    return type(value)({k: cell_item(v, key) for k, v in value.items()})
                
        else:
            return value
    output = cell_output(value)
    if len(output) == 1:
        if key is None: 
            return value[output[0]]
        else:
            if key_before_data:
                return tree_getitem(value, key)
            else:
                try:
                    return value[output[0]]
                except KeyError:
                    return tree_getitem(value, key)
    else:
        if not key_before_data:
            if _data == output[0]:
                return value[_data]
        if key is not None:
            try:
                return tree_getitem(value, key)
            except Exception:
                pass
        if _data == output[0]:
            return value[_data]
        else:            
            return Dict(value)[output]
        

@loop(list, tuple)
def _cell_outputs(c):
    if isinstance(c, cell):
        output = cell_output(c)
        return dictattr({key: c.get(key) for key in output})
    elif isinstance(c, dict):
        try:
            return type(c)(**{k: _cell_outputs(v) for k, v in c.items()})
        except TypeError:
            if isinstance(c, dictable):
                return type(c)(**{str(k): _cell_outputs(v) for k, v in c.items()})
            else:
                return type(c)({k: _cell_outputs(v) for k, v in c.items()})

    else:
        return c

def cell_outputs(value):
    return _cell_outputs(value)
    


def cell_clear(value):
    """
    cell_clear clears a cell of its output so that it contains only the essentil stuff to do its calculations.
    This will be used when we save the cell or we want to recalculate it.
    
    :Example:
    ---------
    >>> from pyg import *    
    >>> a = cell(add_, a = 1, b = 2)
    >>> b = cell(add_, a = 2, b = 3)
    >>> c = cell(add_, a = a, b = b)()
    >>> assert c.data == 8    
    >>> assert c.a.data == 3

    >>> bare = cell_clear(c)
    >>> assert 'data' not in bare and 'data' not in bare.a
    >>> assert bare() == c

    
    :Parameters:
    ------------
    value: obj
        cell (or list/dict of) to be cleared of output

    """
    if isinstance(value, cell):
        return value._clear()
    elif isinstance(value, (tuple, list)):
        return type(value)([cell_clear(v) for v in value])
    elif isinstance(value, dict):
        try:
            return type(value)(**{k : cell_clear(v) for k, v in value.items()})
        except TypeError:
            if isinstance(value, dictable):
                return type(value)(**{str(k): cell_clear(v) for k, v in value.items()})
            else:
                return type(value)({k: cell_clear(v) for k, v in value.items()})
    else:
        return value

def cell_repr(value):
    if isinstance(value, cell):
        return value._repr()
    elif isinstance(value, (tuple, list)):
        return type(value)([cell_repr(v) for v in value])
    elif isinstance(value, dict):
        try:
            return type(value)(**{k : cell_repr(v) for k, v in value.items()})
        except TypeError:
            if isinstance(value, dictable):
                return type(value)(**{str(k): cell_repr(v) for k, v in value.items()})
            else:
                return type(value)({k: cell_repr(v) for k, v in value.items()})
    else:
        return value


def cell_item(value, key = None, key_before_data = False):
    """
    returns an item from a cell (if not cell, returns back the value).
    If no key is provided, will return the output of the cell
    
    :Parameters:
    ------------------
    value : cell or object or list of cells/objects
        cell
    key : str, optional
        The key within cell we are interested in. Note that key is treated as GUIDANCE only. 
        Our strong preference is to return valid output from cell_output(cell)

    key_before_data: bool
        We usually give preference to reserved keyword 'data' when we grab a cell, even if a key is provided
        So if a cell contains both the data and the key, we will return cell[data] by default
        Set key_before_data = True to search for key values first
        
    :Example: non cells
    ---------------------------------
    >>> assert cell_item(1) == 1
    >>> assert cell_item(dict(a=1,b=2)) == dict(a=1,b=2)

    :Example: cells, simple
    ----------------------------
    >>> c = cell(lambda a, b: a+b, a = 1, b = 2)
    >>> assert cell_item(c) is None
    >>> assert cell_item(c.go()) == 3

    """
    return _cell_item(value, key = key, key_before_data = key_before_data)

@loop(list, tuple)
def _cell_go(value, go, mode = 0):
    if isinstance(value, cell):
        if value._awaitable:
            return value
        else:
            return value.go(go = go, mode = mode)
    else:
        if isinstance(value, dict):
            try:
                return type(value)(**{k: _cell_go(v, go = go, mode = mode) for k, v in value.items()})
            except TypeError:
                if isinstance(value, dictable):
                    return type(value)(**{str(k): _cell_go(v, go = go, mode = mode) for k, v in value.items()})
                else:
                    return type(value)({k: _cell_go(v, go = go, mode = mode) for k, v in value.items()})
        else:
            return value

def cell_go(value, go = 0, mode = 0):
    """
    cell_go makes a cell run (using cell.go(go)) and returns the calculated cell.
    If value is not a cell, value is returned.    

    :Parameters:
    ----------------
    value : cell
        The cell (or anything else).
    go : int
        same inputs as per cell.go(go).
        0: run if cell.run() is True
        1: run this cell regardless, run parent cells only if they need to calculate too
        n: run this cell & its nth parents regardless. 
        
    :Returns:
    -------
    The calculated cell
    
    :Example: calling non-cells
    ----------------------------------------------
    >>> assert cell_go(1) == 1
    >>> assert cell_go(dict(a=1,b=2)) == dict(a=1,b=2)

    :Example: calling cells
    ------------------------------------------
    >>> c = cell(lambda a, b: a+b, a = 1, b = 2)
    >>> assert cell_go(c) == c(data = 3)

    """
    return _cell_go(value, go = go, mode = mode)


@loop(list, tuple)
def _cell_load(value, mode):
    if isinstance(value, cell):
        if value._awaitable:
            return value._load(mode) # this is a cheat, we are loading the data synchronously
        else:
            return value.load(mode)
    else:
        if isinstance(value, dict):
            try:
                return type(value)(**{k: _cell_load(v, mode) for k, v in value.items()})
            except TypeError:
                if isinstance(value, dictable):
                    return type(value)(**{str(k): _cell_load(v, mode) for k, v in value.items()})
                else:
                    return type(value)({k: _cell_load(v, mode) for k, v in value.items()})
        else:
            return value

def cell_load(value, mode = 0):
    """
    loads a cell from a database or memory and return its updated values

    :Parameters:
    ----------------
    value : cell
        The cell (or anything else).
    mode: 1/0/-1
        Used by cell.load(mode) -1 = no loading, 0 = load if available, 1 = throw an exception in unable to load

    :Returns:
    -------
    A loaded cell
    
    """
    return _cell_load(value, mode = mode)
        

class cell_func(wrapper):
    """
    cell_func is a wrapper and wraps a function to act on cells rather than just on values    
    When called, it will returns not just the function, but also args, kwargs used to call it.
    
    In order to present the itemized value in the cell, inputs for the function that are cells will:
        1) be loaded (from the persistency layer)
        2) called and calculated
        3) itemized: i.e. cell_item(input)
    
    :Example:
    -------
    >>> from pyg import *
    >>> a = cell(lambda x: x**2, x  = 3)
    >>> b = cell(lambda y: y**3, y  = 2)
    >>> function = lambda a, b: a+b
    >>> self = cell_func(function)
    >>> result, args, kwargs = self(a,b)

    >>> assert result == 8 + 9
    >>> assert kwargs['a'].data == 3 ** 2
    >>> assert kwargs['b'].data == 2 ** 3
    
    :Parameters:
    ------------
    function : callable
        The function to be wrapped
    
    relabels : dict or None
        Allows a redirect of variable names. For example:

        >>> from pyg import * 
        >>> a = cell(a = 1, b = 2, c = 3)
        >>> f = cell_func(lambda x: x+1, relabels = dict(x = 'c')) ## please use key 'c' to grab x value
        >>> assert f(a)[0] == 4
    
    unloaded: list or str
        By defaults, if a cell is in the inputs, it will be loaded (cell.load()) prior to being presented to the function
        If an arg is in unloaded, it will not be loaded

    uncalled: list or str
        By defaults, if a cell is in the inputs, it will be called (cell.call()) prior to being presented to the function
        If an arg is in uncalled, it will not be called

    unitemized: list or str
        By defaults, if a cell is in the inputs, once run, we itemize and grab its data
        If an arg is in unitemized, it will be presented 'as is'
        :Example:
        
        >>> from pyg import * 
        >>> x = cell(passthru, data = 'this is the value presented')
        >>> assert cell_func(lambda x: len(x))(x)[0] == len('this is the value presented') ## function run on item  in x
        >>> assert cell_func(lambda x: len(x), unitemized = 'x')(x)[0] == len(x)           ## function run on x as-is, unitemized
    
    
    """
    def __init__(self, function = None, relabels = None, 
                 unitemized = None, 
                 uncalled = None, 
                 unloaded = None, **other_relabels):
        relabels = relabels or {}
        relabels.update(other_relabels)
        super(cell_func, self).__init__(function = function, 
                                        relabels = relabels, 
                                        unloaded = as_list(unloaded),
                                        unitemized = as_list(unitemized),
                                        uncalled = as_list(uncalled))

    
    @property
    def output(self):
        res = getattr(self.function, _output, None)
        return res or None
    
    def loaded(self, *args, **kwargs):
        go = kwargs.pop('go', 0)
        mode = kwargs.pop('mode', 0)
        loaded_function = cell_item(cell_go(self.function, go))    
        callargs = getcallargs(loaded_function, *args, **kwargs)
        spec = getargspec(loaded_function)
        arg_names = [] if spec.args is None else spec.args
        c = dict(callargs)
        
        varargs = c.pop(spec.varargs) if spec.varargs else []
        loaded_args = cell_load(varargs, mode)
        
        varkw = c.pop(spec.varkw) if spec.varkw else {}
        loaded_varkw = {k : v if k in self.unloaded else cell_load(v, mode) for k, v in varkw.items()}        

        defs = spec.defaults if spec.defaults else []
        params = dict(zip(arg_names[-len(defs):], defs))
        params.update(c)
        loaded_params = {k : v if k in self.unloaded else cell_load(v, mode) for k, v in params.items()}

        return loaded_function, loaded_args, loaded_varkw, loaded_params, arg_names, go
        
        
    def wrapped(self, *args, **kwargs):
        """
        we split the calculation into two stages: 
            1) We load all cells that need loading as inputs to the function
            2) We then call each cell and retrieve the value
        """

        loaded_function, loaded_args, loaded_varkw, loaded_params, arg_names, go = self.loaded(*args, **kwargs)
        
        called_varargs = cell_go(loaded_args , go)
        itemized_varargs = cell_item(called_varargs, _data)

        called_varkw = {k : v if k in self.uncalled else cell_go(v, go) for k, v in loaded_varkw.items()}
        itemized_varkw = {k : v if k in self.unitemized else _cell_item(v, self.relabels[k], True) if k in self.relabels else _cell_item(v, k) for k, v in called_varkw.items()}

        called_params = {k : v if k in self.uncalled else cell_go(v, go) for k, v in loaded_params.items()}
        itemized_params = {k : v if k in self.unitemized else _cell_item(v, self.relabels[k], True) if k in self.relabels else _cell_item(v, k) for k, v in called_params.items()}
        
        args_ = [itemized_params[arg] for arg in arg_names if arg in loaded_params] + list(itemized_varargs)
        res = loaded_function(*args_, **itemized_varkw)
        called_params.update(called_varkw)
        return res, itemized_varargs, called_params
        

def _is_different_except_cells(v1, v2):
    """
    We compare v1 to v2 and check if it is updated..
    cells are compared at run() check rather than at load
    """
    if type(v1)!=type(v2):
        return True
    if isinstance(v1, cell):
        return False
    if isinstance(v1, (list, tuple)):
        if len(v1)!=len(v2):
            return True
        for a1, a2 in zip(v1, v2):
            if _is_different_except_cells(a1, a2):
                return True
        return False
    if isinstance(v1, dict):
        if sorted(v1.keys()) != sorted(v2.keys()):
            return True
        for k in v1:
            if _is_different_except_cells(v1[k], v2[k]):
                return True
        return False
    return not eq(v1, v2)

def _are_any_inputs_updated(new, old):
    for k, v1 in new._inputs.items():
        if k not in old: ## k is a new key
            return True
        v2 = old[k]
        if _is_different_except_cells(v1, v2):
            return True
    v1, v2 = new.function, old.function
    return _is_different_except_cells(v1, v2)


def is_pairs(pairs):
    """
    returns a check if the data is pairs of key-value tuples

    :Parameters:
    ------------
    pairs : tuple
        tuples of key-value tuples.

    :Returns:
    ---------
    bool
    """
    return isinstance(pairs, tuple) and min([isinstance(item, tuple) and len(item) == 2 for item in pairs], default= False)

class cell(dictattr):
    """
    cell is a Dict that can be though of as a node in a calculation graph. 
    The nearest parallel is actually an Excel cell:
    
    - cell contains both its function and its output. cell.output defines the keys where the output is supposed to be
    - cell contains reference to all the function outputs
    - cell contains its locations and the means to manage its own persistency


    :Parameters:
    ------------------
    - function is the function to be called
    - ** kwargs are the function named key value args. NOTE: NO SUPPORT for *args nor **kwargs in function
    - output: where should the function output go?
    
    :Example: simple construction
    -----------------------------------------------
    >>> from pyg import *
    >>> c = cell(lambda a, b: a+b, a = 1, b = 2)
    >>> assert c.a == 1
    >>> c = c.go()
    >>> assert c.output == ['data'] and c.data == 3

    :Example: make output go to 'value' key
    ---------------------------------------------------------------
    >>> c = cell(lambda a, b: a+b, a = 1, b = 2, output = 'value')
    >>> assert c.go().value == 3

    :Example: multiple outputs by function
    -------------------------------------------------------------
    >>> f = lambda a, b: dict(sum = a+b, prod = a*b)
    >>> c = cell(f, a = 1, b = 2, output  = ['sum', 'prod'])
    >>> c = c.go()
    >>> assert c.sum == 3 and c.prod == 2

    :Methods:
    ---------------    
    - cell.run() returns bool if cell needs to be run
    - cell.go() calculates the cell and returns the function with cell.output keys now populated.    
    - cell.load()/cell.save() interface for self load/save persistence
    
    
    """
    _func = cell_func 
    _awaitable = False
    _logger = logger
    
    def __init__(self, function = None, output = None, **kwargs):
        if len(kwargs) == 0 and isinstance(function, (Dict, cell)): 
            kwargs.update(function)
        else:
            kwargs[_function] = function
        if output is not None:
            kwargs[_output] = output
        if _function not in kwargs:
            kwargs[_function] = None
        super(cell, self).__init__(**kwargs)
        outputs = self._output
        overlapping = {key : value for key, value in self._inputs.items() if isinstance(value, cell) and key in outputs}
        if len(overlapping):
            raise ValueError(f'cannot have a cell as an input and at the same time an output: {overlapping}, as it will be over-written on calculation, breaking graph structure')
        self.pull()


    def run(self):
        """
        checks if the cell needs calculation. This depends on the nature of the cell. 
        By default (for cell), if the cell is already calculated so that cell._output exists, then returns False. otherwise True

        Returns
        -------
        bool
            run cell?
            
        :Example:
        ---------
        >>> c = cell(lambda x: x+1, x = 1)
        >>> assert c.run()
        >>> c = c()
        >>> assert c.data == 2 and not c.run()
        """
        if self.get(_updated) is None:
            return True
        output = cell_output(self)
        if _data in output:
            return self.get(_data) is None
        else:
            for o in output:
                if self.get(o) is None:
                    return True
        return False
    

    def save(self):
        """
        Saves the cell for persistency. For an in-memory cell, this is implemented by storing cell._address in the GRAPH

        :Returns:
        -------
        cell
            self, saved.
            
            
        Example:
        -------
        >>> from pyg import * 
        >>> a = pd.Series([1,2,3], drange(2))
        >>> b = 3
        >>> c = cell(add_, a = a, b = b, name = 'james', surname = 'cohen', db = 'c:/test/%name/%surname.pickle')()

        """
        writer = self._writer()
        if writer is not None:
            w = as_writer(writer)[0]
            doc = {k: self[k] for k in self._output if k in self}
            if len(doc)>0:
                pk = self._pk
                keys = {k : self[k] for k in pk if k in self}
                if len(keys) < len(pk):
                    logger.info(f'cannot save cell as need {pk} but only have {keys}')
                else:
                    doc.update(keys)
                    _ = w(doc)        
        return self


    @property
    def _pk(self):
        """
        res = 'c:/%a/%b.parquet'
        """
        res = self.get(_pk, self.get(_db))
        if isinstance(res, str) and '%' in res:
            keys = [key.split('.')[0].split('/')[0].split('\\')[0] for key in res.split('%')[1:]]
        else:
            keys = as_list(res)
        return ulist(sorted(as_list(keys)))
    
    def _writer(self):
        db = self.get(_db)
        if isinstance(db, str) and '%' in db:
            return db
        return None

        
    def load(self, mode = 0):
        """
        Loads the cell from in-memory GRAPH using primary keys of cell.
        
        :Parameters:
        -------------
        mode : int
            if set to -1, will delete the cell from the GRAPH memory
            if set to +1, will raise an error if not found in GRAPH
            if set to 0, will load the data if in GRAPH. If not in graph, will do nothing and return self.

        :Returns:
        -------
        cell
            self, updated with values from database.
        """
        address = self._address
        if not address:
            return self
        GRAPH = get_cache('GRAPH')
        if mode == -1:
            if address in GRAPH:
                del GRAPH[address]
            return self
        if address in GRAPH:
            saved = GRAPH[address] 
            self_updated = self.get(_updated)
            saved_updated = saved.get(_updated)
            if self_updated is None or (saved_updated is not None and saved_updated > self_updated):
                excluded_keys = (self /  None).keys() - self._output - _updated
            else:
                excluded_keys = (self /  None).keys()
            if is_strs(mode):
                excluded_keys += as_list(mode)
            elif is_date(mode):
                excluded_keys += [_id]
            update = (saved / None) - excluded_keys
            updated_inputs = [k for k, v in self._inputs.items() if k in saved and v is not None and not isinstance(v, cell) and not eq(saved[k], v)]
            self.update(update)
            if len(updated_inputs):
                self[_updated] = None
        db = self.get(_db)
        if isinstance(db, str) and '%' in db:
            on_file = pd_read_root(db, self, self._output)
            if len(on_file):
                self.update(on_file)
        return self
            
    def __call__(self, go = 0, mode = 0, **kwargs):
        """
        1) updates the cell using kwargs
        2) loads the data from the persistency layewr using mode policy
        3) runs the cells using go-policy

        Parameters
        ----------
        go : int, optional
            execution policy for cell. The default is 0.
        mode : int, optional
            load policy. The default is 0.
        **kwargs : dict
            additional variables to be added to the cell.

        Returns
        -------
        cell
            the loaded & calculated cell.

        """
        return (self + kwargs).load(mode = mode).go(go = go, mode = mode)


    @property
    def fullargspec(self):
        return None if self.function is None or not callable(self.function) else getargspec(self.function) 
    
    @property
    def _args(self):
        """
        returns the keyword arguments within the cell that MAY be presented to the cell.function 
        Does not 
        """
        return [] if self.function is None else getargs(self.function) if callable(self.function) else []
    
    @property
    def _inputs(self):
        """
        returns a dict of the keys and values in the cell that can be presented to the function
        
        :Example:
        ---------
        >>> from pyg import * 
        >>> c = cell(lambda a, b = 1: a + b , a = 2, b = 3)
        >>> assert c._inputs == {'a': 2, 'b': 3} and c._args == ['a', 'b']
        >>> c = cell(lambda a, b = 1: a + b , a = 2)
        >>> assert c._inputs == {'a': 2} and c._args == ['a', 'b']
            
        """
        spec = self.fullargspec
        if spec is None:
            return {}
        res = {key : self[key] for key in spec.args + spec.kwonlyargs if key in self}
        if spec.varargs and spec.varargs in self:
            res[spec.varargs] = self[spec.varargs]
        if spec.varkw and spec.varkw in self:
            varkw = self[spec.varkw]
            if not isinstance(varkw, dict):
                raise ValueError('%s in the cell must be a dict as that parameter is declared by function as varkw' % spec.varkw)
            res.update(varkw)
        return res

        # args = self._args
        # return {key : self[key] for key in args if key in self}
    
    
    @property
    def _output(self):
        """
        returns the keys within the cell where the output from the function will be stored.
        This can be set in two ways:
        
        :Example:
        ----------
        >>> from pyg import *
        >>> f = lambda a, b : a + b
        >>> c = cell(f, a = 2, b = 3)
        >>> assert c._output == ['data']
        
        >>> c = cell(f, a = 2, b = 3, output = 'x')
        >>> assert c._output == ['x'] and c().x == 5            

        :Example: output for functions that return dicts
        ---------

        >>> f = lambda a, b : dict(sum = a + b, prod = a*b) ## function returns a dict!
        >>> c = cell(f, a = 2, b = 3)()
        >>> assert c.data['sum'] == 5 and c.data['prod'] == 6 ## by default, the dict goes to 'data' again

        >>> c = cell(f, a = 2, b = 3, output = ['sum', 'prod'])() ## please, send the output to these keys...
        >>> assert c['sum'] == 5 and c['prod'] == 6 ## by default, the dict goes to 'data' again

        >>> f.output = ['sum', 'prod'] ## we make the function declare that it returns a dict with these keys        
        >>> c = cell(f, a = 2, b = 3)() ## now the cell does not need to set this
        >>> assert c.sum == 5 and c.prod == 6 
        """
        return cell_output(self)
    

    @property
    def _address(self):
        """
        :Example:
        ----------
        >>> from pyg import *
        >>> self = cell(pk = 'key', key = 1)
        >>> self._address
        
        :Returns:
        -------
        tuple
            returns a tuple representing the unique address of the cell.
        """
        pk = self._pk
        if not pk:
            return None
        missing = [k for k in pk if k not in self]
        if len(missing):
            self._logger.warning(f'missing these keys:{missing} from the primary keys {pk}')
            return None        
        return tuple([(key, self[key]) for key in pk])


    def _clear(self):
        res = self if self.function is None else self - self._output
        return type(self)(**cell_clear(dict(res)))
    

    @property
    def _function(self):
        function = self.function if isinstance(self.function, self._func) else self._func(self.function)
        return function
    
    def _go_finally(self, address, mode):
        self[_updated] = now = datetime.datetime.now()
        if address:
            address = self._address
            UPDATED = get_cache('UPDATED')
            UPDATED[address] = now
            if mode>=0:
                GRAPH = get_cache('GRAPH')
                GRAPH[address] = self.copy()
        return self
    
    def _go(self, go = 0, mode = 0):
        """

        Parameters
        ----------
        go : int, optional
            do we want to force the calculation? The default is 0.
        mode : int, optional
            The mode of loading for the parameters. The default is 0.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        address = self._address
        if (go!=0 or self.run()):
            if callable(self.function):
                spec = self.fullargspec
                if spec is None: ##shouldn't happen as self.function callable
                    return self
                if address:
                    pairs = ', '.join([("%s = '%s'" if isinstance(value, str) else "%s = dt('%s')" if is_date(value) else "%s = %s")%(key, value) for key, value in address])
                    msg = "get_cell(%s)()"%pairs
                else:
                    msg = str(address)
                self._logger.info(msg)
                args = spec.args
                kwonlyargs = spec.kwonlyargs
                kwonlydefaults = spec.kwonlydefaults or {}
                defaults = [] if spec.defaults is None else spec.defaults
                varargs = [] if spec.varargs is None or spec.varargs not in self else as_list(self[spec.varargs])
                varkw = {} if spec is None or spec.varkw is None or spec.varkw not in self else self[spec.varkw]
                if not isinstance(varkw, dict):
                    raise ValueError('%s in the cell must be a dict as that parameter is declared by function as varkw' % spec.varkw)
                if len(args) and args[0] == 'self':
                    self._logger.warning('cell is not designed to work with methods or objects which take "self" as first argument')
                    required_args = [self[arg] for arg in args[1: len(args)-len(defaults)]]
                else:
                    required_args = [self[arg] for arg in args[: len(args)-len(defaults)]]                
                defaulted_args = [self.get(arg, default) for arg, default in zip(args[len(args)-len(defaults):], defaults)]
                kwargs = {arg : self.get(arg, kwonlydefaults[arg]) if arg in kwonlydefaults else self[arg] for arg in kwonlyargs}
                kwargs.update(varkw)
                function = self._function
                res, called_args, called_kwargs = function(*required_args, *defaulted_args, *varargs, go = go-1 if is_num(go) and go>0 else go, mode = mode, **kwargs)
                c = self.copy()
                output = cell_output(c)
                if output is None:
                    c[_data] = res
                elif len(output)>1:
                    res = {o : res[o] for o in output}
                    for o in output:
                        c[o] = res[o]
                else:
                    c[output[0]] = res
                tss = list_instances(res, is_ts)
                latests = [ts.index[-1] for ts in tss if len(ts)>0]
                c[_latest] = max(latests, default = None)
                return c._go_finally(address, mode)
            elif isinstance(self.function, (list, dict)):
                functions = self.function
                res = self.copy()
                if isinstance(functions, list):
                    keys = [getargs(function)[0] for function in functions[1:]] + [_data]
                    funcs = functions
                else:
                    keys = functions.keys()
                    funcs = functions.values()
                for key, function in zip(keys, funcs):
                    res.function = function
                    assert cell_output(res) in (None, [_data])
                    res = res._go(1, mode)
                    res[key] = res[_data]
                value = res[_data]
                for key in keys:
                    del res[key]
                res[_data] = value
                res.function = functions
                return res._go_finally(address, mode)

        if address and mode>=0:
            GRAPH = get_cache('GRAPH')
            GRAPH[address] = self.copy()
        return self
            

    @property
    def _latest(self):
        """
        We cache ._latest as a visible parameter called latest
        
        >>> from pyg import * 
        >>> a = cell(data = pd.Series(np.arange(100), drange(99)).astype(float))
        >>> b = latest_cell(ewma, a = a , n = 10)        
        >>> c = latest_cell(ewma, a = b, n = 20)

        >>> c = c()
        >>> c.data
        
        a.data = pd.Series(np.arange(101), drange(100)).astype(float)
        a._latest
        a.data        
        a.latest
        
        b.run()
        c.run()
        """
        output = cell_output(self)
        outputs = [self[o] for o in output if o in self.keys()]
        tss = list_instances(outputs, is_ts)
        latests = [ts.index[-1] for ts in tss if len(ts)>0]
        latest = max(latests, default = None)
        if latest is None:
            return self.get(_latest)
        else:
            self[_latest] = latest
            return latest

    def go(self, go = 1, mode = 0, **kwargs):
        """
        calculates the cell (if needed). By default, will then run cell.save() to save the cell. 
        If you don't want to save the output (perhaps you want to check it first), use cell._go()

        :Parameters:
        ------------
        go : int, optional
            a parameter that forces calculation. The default is 0.
            go = 0: calculate cell only if cell.run() is True
            go = 1: calculate THIS cell regardless. calculate the parents only if their cell.run() is True
            go = 2: calculate THIS cell and PARENTS cell regardless, calculate grandparents if cell.run() is True etc.
            go = -1: calculate the entire tree again.
        
        mode: int, optional
            how do you want to load the parameters that are the inputs to the function.
            
        **kwargs : parameters
            You can actually allocate the variables to the function at runtime

        Note that by default, cell.go() will default to go = 1 and force a calculation on cell while cell() is lazy and will default to assuming go = 0

        :Returns:
        -------
        cell
            the cell, calculated
        
        :Example: different values of go
        ---------------------
        >>> from pyg import *
        >>> f = lambda x=None,y=None: max([dt(x), dt(y)])
        >>> a = cell(f)()
        >>> b = cell(f, x = a)()
        >>> c = cell(f, x = b)()
        >>> d = cell(f, x = c)()

        >>> e = d.go()
        >>> e0 = d.go(0)
        >>> e1 = d.go(1)
        >>> e2 = d.go(2)
        >>> e_1 = d.go(-1)

        >>> assert not d.run() and e.data == d.data 
        >>> assert e0.data == d.data 
        >>> assert e1.data > d.data and e1.x.data == d.x.data
        >>> assert e2.data > d.data and e2.x.data > d.x.data and e2.x.x.data == d.x.x.data
        >>> assert e_1.data > d.data and e_1.x.data > d.x.data and e_1.x.x.data > d.x.x.data

        :Example: adding parameters on the run
        --------------------------------------
        >>> c = cell(lambda a, b: a+b)
        >>> d = c(a = 1, b =2)
        >>> assert d.data == 3


        """
        res = (self + kwargs)
        if res.run() or (is_date(go) and self.get(_updated, go) <= go)  or (is_num(go) and go!=0):
            res = res._go(go = go, mode = mode)
            address = res._address
            if address in UPDATED:
                res[_updated] = UPDATED[address] 
            else: 
                res[_updated] = datetime.datetime.now()
            return res.save()
        else:
            address = res._address
            if address and mode>=0:
                GRAPH[address] = res.copy()
            return res
        
    
    def copy(self):
        return copy(self)

    
    def _repr(self):
        pk = self._pk 
        return tree_repr(type(self)({k:self.get(k) for k in pk + ['db', 'function'] + self._output}))


    def __repr__(self):
        return '%s\n%s'%(self.__class__.__name__,tree_repr({k : cell_repr(v) for k, v in self.items()}))

    def pull(self):
        """
        pull works together with push to ensure that if an upstream cell has updated, downward cells *who register to pull data* gets UPDATED
        pull does not actually calculate any cell, it simply registers the parents of a cell in the DAG
        
        :Example:
        ---------
        >>> from pyg import * 
        >>> from pyg_base._cell import _GAD
        >>> c = cell(add_, a = 1, b = 2, pk = 'key', key = 'c')()
        >>> d = cell(add_, a = 1, b = c, pk = 'key', key = 'd')()
        >>> e = cell(add_, a = c, b = d, pk = 'key', key = 'e')()
        >>> f = cell(add_, a = e, b = d, pk = 'key', key = 'f')()
                

        Returns
        -------
        cell
            DESCRIPTION.

        """
        inputs = cell_inputs(self)
        if len(inputs) == 0:
            return self
        inputs = set([c._address for c in inputs])
        me = self._address
        dag = get_DAG()
        _GAD = get_GAD()
        if me in _GAD:
            to_remove = _GAD[me] - inputs
        else:
            to_remove = []
        for key in inputs:
            add_edge(key, me, dag = dag)
        for key in to_remove:
            del_edge(key, me, dag = dag)
        _GAD[me] = inputs
        return self

    def push(self, go = 1):
        me = self._address
        res = self.go()
        generations = topological_sort(get_DAG(), [me])['gen2node']
        for i, children in sorted(generations.items())[1:]: # we skop the first generation... we just calculated it
            GRAPH.update({child : GRAPH[child].go(go = go) for child in children})            
            for child in children:
                del UPDATED[child]
        if me in UPDATED:
            del UPDATED[me]
        return res
    
    def bind(self, **bind):
        """
        bind adds key-words to the primary keys of a cell

        :Parameters:
        ----------
        bind : dict
            primary keys and their values.
            The value can be a callable function, transforming existing values

        :Returns:
        -------
        res : cell
            a cell with extra binding as primary keys.

        :Example:
        ---------
        >>> from pyg import *
        >>> c = cell(a = 1)()
        >>> assert GRAPH == {} # c has no primary keys so it is not in the graph
        >>> d = c.bind(key = 'key').go()
        >>> assert d.pk == ['key']
        >>> assert d._address in GRAPH
        >>> e = d.bind(key2 = lambda key: key + '1')()
        >>> assert e.pk == ['key', 'key2']
        >>> assert e._address == (('key', 'key'), ('key2', 'key1'))
        >>> assert e._address in GRAPH
        """
        pk = sorted(set(as_list(self.get(_pk))) | set(bind.keys()))
        res = Dict({key: self.get(key) for key in pk})
        res = res(**bind)
        res[_pk] = pk
        return self + res



class updated_cell(cell):
    """ 
    An updated_cell will only update its "updated" and will only save itself, once the value in its output has changed
    """
    def go(self, go = 1, mode = 0, **kwargs):
        """
        calculate and then save the new value IF DIFFERENT TO PREVIOUS value
        
        :Example:
        --------
        >>> from pyg import * ; import time
        >>> a = cell(data = 1)
        >>> b = updated_cell(add_, a = a, b = 3)()
        >>> c = cell(add_, a = a, b = 30)()

        >>> time.sleep(1)

        >>> b2 = b.go()
        >>> c2 = c.go()
        >>> assert c2.updated > c.updated
        >>> assert b2.updated == b.updated
        """
        res = (self + kwargs)
        if res.run() or (is_date(go) and self.get(_updated, go) <= go)  or (is_num(go) and go!=0):
            old = {key : res[key] for key in cell_output(res) if key in res}
            res = res._go(go = go, mode = mode)
            new = {key : res[key] for key in cell_output(res) if key in res}
            if eq(new, old):
                return res
            address = res._address
            if address in UPDATED:
                res[_updated] = UPDATED[address] 
            else: 
                res[_updated] = datetime.datetime.now()
            return res.save()
        else:
            address = res._address
            if address and mode>=0:
                GRAPH[address] = res.copy()
            return res
        
def cell_inputs(value, types = cell):
    """
    returns a list of inputs for a cell of type 'types'

    Parameters
    ----------
    value : cell
        cell.
    types : types, optional
        search for inputs of that type. The default is cell.

    Returns
    -------
    list.

    """
    if isinstance(value, cell):
        return list_instances(value._inputs, types)
    else:
        return list_instances(value, types)



