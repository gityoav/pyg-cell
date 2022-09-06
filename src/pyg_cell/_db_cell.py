from pyg_base import is_date, ulist, logger, is_str, as_list, get_cache, Dict, dictable, list_instances
from pyg_encoders import cell_root, root_path, pd_read_parquet, pickle_load, pd_read_csv, dictable_decode
from pyg_npy import pd_read_npy
from pyg_cell._types import _get_mode, _get_qq, DBS, QQ
from pyg_cell._cell import cell, cell_clear, cell_item, cell_output, cell_func
from pyg_cell._dag import get_DAG, get_GAD, descendants

from functools import partial
# import networkx as nx
import os

_readers = dict(parquet = pd_read_parquet, 
                pickle = pickle_load, 
                npy = pd_read_npy, 
                csv = pd_read_csv,
                dictable = dictable_decode)
try:
    from pyg_sql import sql_loads
    _readers['sql'] = sql_loads
except Exception:
    pass

_deleted = 'deleted'
_id = '_id'
_pk = 'pk'
_db = 'db'
_updated = 'updated'
_function = 'function'
_doc = 'doc'

__all__ = ['db_load', 'db_save', 'db_cell', 'cell_push', 'cell_pull', 'get_cell', 'load_cell', 'get_data', 'load_data']

def get_GRAPH():
    return get_cache('GRAPH')

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


class db_cell_func(cell_func):
    """
    same as cell_func but implements a check that all cells internally point to the same environment variables

    """
    def wrapped(self, *args, **kwargs):
        env = self.get('env')
        if env is not None:
            cells = list_instances((args, kwargs), db_cell)
            for c in cells:
                if 'db' in c.keys():
                    keywords = c['db'].keywords
                    for k,v in env.items():
                        if keywords.get(k)!=v:
                            raise ValueError('cell is pointing to %s=%s while all cells must point to %s'%(k,keywords.get(k),v))
        return  super(db_cell_func, self).wrapped(*args, **kwargs)           


        
def db_save(value):
    """
    saves a db_cell from the database. Will iterates through lists and dicts

    :Parameters:
    ------------
    value: obj
        db_cell (or list/dict of) to be loaded 
        
    :Example:
    ---------
    >>> from pyg import *
    >>> db = partial(mongo_table, table = 'test', db = 'test', pk = ['a','b'])
    >>> c = db_cell(add_, a = 2, b = 3, key = 'test', db = db)
    >>> c = db_save(c)    
    >>> assert get_cell('test', 'test', a = 2, b = 3).key == 'test'
        

    """
    if isinstance(value, db_cell):
        return value.save()
    elif isinstance(value, (tuple, list)):
        return type(value)([db_save(v) for v in value])
    elif isinstance(value, dict):
        return type(value)(**{k : db_save(v) for k, v in value.items()})
    else:
        return value

def db_load(value, mode = 0):
    """
    loads a db_cell from the database. Iterates through lists and dicts
    
    :Parameters:
    ------------
    value: obj
        db_cell (or list/dict of) to be loaded 
    
    mode: int
        loading mode -1: dont load, +1: load and throw an exception if not found, 0: load if found
    
    """
    if isinstance(value, db_cell):
        return value.load(mode)
    elif isinstance(value, (tuple, list)):
        return type(value)([db_load(v, mode = mode) for v in value])
    elif isinstance(value, dict):
        return type(value)(**{k : db_load(v, mode = mode) for k, v in value.items()})
    else:
        return value
        
def _load_asof(table, kwargs, deleted, qq = None):  
    t = table.inc(kwargs)
    live = t
    l = len(live)
    if deleted in (False, None): # we just want live values
        if l == 0:
            raise ValueError('no undeleted cells found matching %s in \n%s'%(kwargs, live))        
        elif l>1:
            raise ValueError('multiple cells found matching %s in \n%a'%(kwargs, live))
        return live[0]
    else:  
        qq = _get_qq(qq, table)
        past = t.deleted if deleted is True else t.deleted.inc(qq['deleted'] > deleted) ## it is possible to have cells with deleted in self
        p = len(past)
        if p == 1:
            return past[0]
        elif p > 1:
            if deleted is True:
                raise ValueError('multiple historic cells are avaialble %s with these dates: %s. set delete = DATE to find the cell on that date in \n%s'%(kwargs, past.deleted, past))
            else:
                return past.sort('deleted')[0]
        else:    ## no records found in past, we go to the deleted history
            history = t.deleted if deleted is True else t.deleted.inc(qq['deleted'] > deleted) #cells deleted after deleted date
            h = len(history)
            if h == 0:
                if l > 0:
                    raise ValueError('no deleted cells found matching %s but a live one exists. Set deleted = False to get it, in \n%s'%(kwargs, history))        
                else:                   
                    raise ValueError('no deleted cells found matching %s in \n%s'%(kwargs, history))        
            elif h>1:
                if deleted is True:
                    raise ValueError('multiple historic cells are avaialble %s with these dates: %s. set delete = DATE to find the cell on that date, in \n%s'%(kwargs, history.deleted, history))
                else:
                    return history.sort('deleted')[0]
            else:
                return history[0]

import datetime
def _is_primitive(value):
    if isinstance(value, (tuple, list)):
        return min([_is_primitive(v) for v in value], default = True)
    elif isinstance(value, dict):
        return min([_is_primitive(v) for v in value.values()], default = True)
    else:
        return value is None or isinstance(value, (type, bool, str, int, float, datetime.datetime, datetime.date, partial))

class db_cell(cell):
    """
    a db_cell is a specialized cell with a 'db' member pointing to a database where cell is to be stored.    
    We use this to implement save/load for the cell.
    
    It is important to recognize the duality in the design:
    - the job of the cell.db is to be able to save/load based on the primary keys.
    - the job of the cell is to provide the primary keys to the db object.
    
    The cell saves itself by 'presenting' itself to cell.db() and say... go on, load my data based on my keys. 
    
    :Example: saving & loading
    --------------------------
    >>> from pyg import *
    >>> people = partial(mongo_table, db = 'test', table = 'test', pk = ['name', 'surname'])
    >>> anna = db_cell(db = people, name = 'anna', surname = 'abramzon', age = 46).save()
    >>> bob  = db_cell(db = people, name = 'bob', surname = 'brown', age = 25).save()
    >>> james = db_cell(db = people, name = 'james', surname = 'johnson', age = 39).save()


    Now we can pull the data directly from the database

    >>> people()['name', 'surname', 'age'][::]
    >>> dictable[3 x 4]
    >>> _id                     |age|name |surname 
    >>> 601e732e0ef13bec9cd8a6cb|39 |james|johnson 
    >>> 601e73db0ef13bec9cd8a6d4|46 |anna |abramzon
    >>> 601e73db0ef13bec9cd8a6d7|25 |bob  |brown       

    db_cell can implement a function:

    >>> def is_young(age):
    >>>    return age < 30
    >>> bob.function = is_young
    >>> bob = bob.go()
    >>> assert bob.data is True

    When run, it saves its new data to Mongo and we can load its own data:

    >>> new_cell_with_just_db_and_keys = db_cell(db = people, name = 'bob', surname = 'brown')
    >>> assert 'age' not in new_cell_with_just_db_and_keys 
    >>> now_with_the_data_from_database = new_cell_with_just_db_and_keys.load()
    >>> assert now_with_the_data_from_database.age == 25

    >>> people()['name', 'surname', 'age', 'data'][::]
    >>>  dictable[3 x 4]
    >>> _id                     |age|name |surname |data
    >>> 601e732e0ef13bec9cd8a6cb|39 |james|johnson |None
    >>> 601e73db0ef13bec9cd8a6d4|46 |anna |abramzon|None
    >>> 601e73db0ef13bec9cd8a6d7|25 |bob  |brown   |True
    >>> people().reset.drop()    

    """

    def __init__(self, function = None, output = None, db = None, **kwargs):
        if db is not None:
            if not isinstance(db, partial):
                raise ValueError('type of db is: %s while db must be a *partial* of a function like mongo_table initializing a mongo cursor' % type(db))
            non_primitives = {key : value for key, value in db.keywords.items() if not _is_primitive(value)}
            if len(non_primitives):
                raise ValueError('partial construction of cell must be from primitive paramters. but these are not: %s'%non_primitives)
            super(db_cell, self).__init__(function = function, output = output, db = db, **kwargs)
        else:
            self[_db] = None
            super(db_cell, self).__init__(function = function, output = output, **kwargs)

    _func = db_cell_func
    
    @property
    def _function(self):
        if 'db' not in self:
            function = self.function if isinstance(self.function, cell_func) else cell_func(self.function)
        else:
            keywords = self.db.keywords
            env = {}
            for k in ['server', 'url']:
                if k in keywords:
                    env[k] = keywords[k]
            if env:
                if isinstance(self.function, db_cell_func):
                    function  = self.function
                elif isinstance(self.function, cell_func):
                    f = self.function
                    function = db_cell_func(f.function, 
                                                relabels = f.relabels, 
                                                unloaded = f.unloaded, 
                                                unitemized = f.unitemized,
                                                uncalled = f.uncalled)
                else:
                    function = db_cell_func(self.function)
                function.env = env
            else:
                function = self.function if isinstance(self.function, cell_func) else cell_func(self.function)                
        return function

    @property
    def _pk(self):
        if self.get(_db) is None:
            return super(db_cell, self)._pk
        else:
            return self.db.keywords.get(_pk)        

    @property
    def _address(self):
        """
        :Example:
        ----------
        >>> from pyg import *
        >>> self = db_cell(db = partial(mongo_table, 'test', 'test', pk = 'key'), key = 1)
        >>> db = self.db()
        >>> self._address
        >>> self._reference()
        >>> self.get('key')
        
        :Returns:
        -------
        tuple
            returns a tuple representing the unique address of the cell.
        """
        if self.get(_db) is None:
            return super(db_cell, self)._address
        db = self.db()
        db_address = db.address
        db_dict = dict(db_address)
        return db_address + tuple([(key, self.get(key)) for key in db._pk if key not in db_dict])


    def _clear(self):
        """
        Removes most of the data from the cell. Just keeps it so that we have enough data to load it back from the database

        :Returns:
        -------
        db_cell
            skeletal reference to the database

        """
        if self.get(_db) is None: 
            return super(db_cell, self)._clear()
        else:
            return self[[_db, _function, _updated] + self.db()._pk] if _updated in self else self[[_db, _function] + self.db()._pk]

    def save(self):
        if self.get(_db) is None:
            return super(db_cell, self).save()
        address = self._address
        doc = (self - _deleted)
        db = self.db()
        missing = ulist(db._pk) - self.keys()
        if len(missing):
            logger.warning('WARN: document not saved as some keys are missing %s'%missing)
            return self            
        ref = type(doc)(**cell_clear(dict(doc)))
        try:
            updated = db.update_one(ref)
        except Exception:
            updated = db.update_one(ref - db._ids)
        for i in db._ids:
            doc[i] = updated.get(i)
        get_GRAPH()[address] = doc
        return doc
                
        
    def load(self, mode = 0):
        """
        loads a document from the database and updates various keys.
        
        :Persistency:
        -------------
        Since we want to avoid hitting the database, there is a singleton GRAPH, a dict, storing the cells by their address.
        Every time we load/save from/to Mongo, we also update GRAPH.
        
        We use the GRAPH often so if you want to FORCE the cell to go to the database when loading, use this:

        >>> cell.load(-1) 
        >>> cell.load(-1).load(0)  # clear GRAPH and load from db
        >>> cell.load([0])     # same thing: clear GRAPH and then load if available

        :Merge of cached cell and calling cell:
        ----------------------------------------
        Once we load from memory (either MongoDB or GRAPH), we tree_update the cached cell with the new values in the current cell.
        This means that it is next to impossible to actually *delete* keys. If you want to delete keys in a cell/cells in the database, you need to:
        
        >>> del db.inc(filters)['key.subkey']

        :Parameters:
        ----------
        mode : int , dataetime, optional
            if -1, then does not load and clears the GRAPH
            if 0, then will load from database if found. If not found, will return original document
            if 1, then will load, if doc not found, data from files (equivalent to db_cell.load_output)
            if mode is a date, will return the version alive at that date 
            The default is 0.
            
            IF you enclose any of these in a list, then GRAPH is cleared prior to running and the database is called.
    
        :Returns:
        -------
        document

        """
        if self.get(_db) is None:
            return super(db_cell, self).load(mode = mode)
        if isinstance(mode, (list, tuple)):
            if len(mode) == 0:
                return self.load()
            if len(mode) == 1:
                return self.load(mode[0])
            if len(mode) == 2 and mode[0] == -1:
                res = self.load(-1)
                return res.load(mode[-1])
            else:
                raise ValueError('mode can only be of the form [], [mode] or [-1, mode]')
        db = self.db(mode = 'w')
        pk = ulist(db._pk)
        missing = pk - self.keys()
        if len(missing):
            logger.warning('WARN: document not loaded as some keys are missing %s'%missing)
            return self            
        address = self._address
        kwargs = {k : self[k] for k in pk}
        graph = get_GRAPH()
        if mode == -1:
            if address in graph:
                del graph[address]
            return self
        if address not in graph: # we load from the database
            if is_date(mode):
                doc = _load_asof(db, kwargs, deleted = mode)
                graph[doc._address] = doc
            else:
                try:
                    doc = _load_asof(table = db, kwargs = kwargs, deleted = False, qq = None)
                    graph[doc._address] = doc
                except Exception:
                    if mode in (1, True):
                        return self.load_output()
                    else:
                        return self

        if address in graph:
            saved = graph[address] 
            self_updated = self.get(_updated)
            saved_updated = saved.get(_updated)
            if self_updated is None or (saved_updated is not None and saved_updated > self_updated):
                excluded_keys = (self /  None).keys() - self._output - _updated
            else:
                excluded_keys = (self /  None).keys()
            if is_date(mode):
                excluded_keys += [_id]
            update = (saved / None) - excluded_keys
            self.update(update)
        return self        

    def load_output(self):
        """
        We may save the cell data on file system rather than in the database. 
        This allows us to load some of the data even if the database itself has been wiped clean.
        This does not support a full data reload at the moment, but if the cells outputs are simple dataframes, then cell.load_output will reconstruct it. 
        
        """
        root = cell_root(self)
        if root is None:
            return self
        path = root_path(self, root)
        fmt = path.split('.')[-1].lower()
        n = len(fmt) + 1
        for output in cell_output(self):
            for suffix, reader in _readers.items():
                filename = path[:-n] + '/%s.%s'%(output, suffix)            
                if os.path.exists(filename):
                    self[output] = reader(filename)
                elif suffix == 'sql' and is_str(self.writer) and self.writer.endswith('sql'):
                    try:
                        filename = path[:-n] + '/%s'%(output)            
                        self[output] = reader(filename)
                    except Exception:
                        pass
        return self

    def push(self, go = 1):        
        me = self._address
        res = self.go() # run me on my own as I am not part of the push
        cell_push(me, exc = 0, go = go)
        UPDATED = get_cache('UPDATED')
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
        >>> db = partial(mongo_table, 'test', 'test', pk = 'key')
        >>> c = db_cell(passthru, data = 1, db = db, key = 'old_key')()
        >>> d = c.bind(key = 'key').go()
        >>> assert d.pk == ['key']
        >>> assert d._address in GRAPH
        >>> e = d.bind(key2 = lambda key: key + '1')()
        >>> assert e.pk == ['key', 'key2']
        >>> assert e._address == (('key', 'key'), ('key2', 'key1'))
        >>> assert e._address in GRAPH
        """
        db = self.get(_db)
        if db is None:
            return super(db_cell, self).bind(**bind)
        else:
            kw = self.db.keywords
            for k in bind: # we want to be able to override tables/db/url
                if k in ['db', 'table', 'url']:
                    kw[k] = bind.pop(k)
            pk = sorted(set(as_list(kw.get(_pk))) | set(bind.keys()))
            kw[_pk] = pk
            db = partial(db.func, *db.args, **kw)
            res = Dict({key: self.get(key) for key in pk})
            res = res(**bind)
            res[_db] = db
            return self + res


def cell_push(nodes = None, exc = None, go = 1):
    UPDATED = get_cache('UPDATED')
    GRAPH = get_cache('GRAPH')
    if nodes is None:
        nodes = UPDATED.keys()
    children = [child for child in descendants(get_DAG(), nodes, exc = exc) if child is not None]
    for child in children:
        GRAPH[child] = (GRAPH[child] if child in GRAPH else get_cell(**dict(child))).go(go = go)
    for child in children:
        del UPDATED[child]


def cell_pull(nodes, types = cell):
    for node in as_list(nodes):
        node = node.pull()
        _GAD = get_GAD()
        children = [get_cell(**dict(a)) for a in _GAD.get(node._address,[])]        
        cell_pull(children, types)
    return None        



def _get_cell(table = None, db = None, url = None, server = None, deleted = None, _from_memory = None, doc = None, **kwargs):
    """
    retrieves a cell from a table in a database based on its key words. In addition, can look at earlier versions using deleted.
    It is important to note that this DOES NOT go through the cache mechanism but goes to the database directly every time.

    :Parameters:
    ----------
    table : str, an address in the form of pairs, or a partial to sql_table on mongo_table
        name of table (Mongo collection). 
        alternatively, you can just provide an address
        or a partial to the actual table
        
    db : str
        name of database.
    url : str, optional
        mongodb server location. The default is None.
    server : str or True, optional
        sql server location. The default is None (default to mongodb). Set to True to map to default sql server configured in cfg['sql_server']
    deleted : datetime/None, optional
        The date corresponding to the version of the cell we want
        None = live cell
        otherwise, the cell that was first deleted after this date.
    **kwargs : keywords
        key words for grabbing the cell.

    :Returns:
    -------
    The document

    :Example:
    ---------
    >>> from pyg import *
    >>> people = partial(mongo_table, db = 'test', table = 'test', pk = ['name', 'surname'])
    >>> brown = db_cell(db = people, name = 'bob', surname = 'brown', age = 39).save()
    >>> assert get_cell('test','test', surname = 'brown').name == 'bob'
        
    """
    GRAPH = get_cache('GRAPH')
    if is_pairs(table):
        params = dict(table)
        params.update({key: value for key, value in dict(db = db, 
                                                         url = url, 
                                                         server = server,
                                                         deleted = deleted,
                                                         _from_memory = _from_memory
                                                         ).items() if value is not None})
        params.update(kwargs)
        return _get_cell(**params)
    
    pk = kwargs.pop('pk', None)
    if pk is None:
        address = kwargs_address = tuple(sorted(kwargs.items()))
    else:
        pk = sorted(as_list(pk))
        address = kwargs_address = tuple([(key, kwargs.get(key)) for key in pk]) 
    
    schema = kwargs.pop('schema', None)
    mode = _get_mode(url, server, schema = schema)    
    if table is not None:
        if isinstance(table, partial):
            t = table()
            if table.func == DBS.get('sql'):
                mode = 'sql'
                qq = t.table.c
            elif table.func == DBS.get('mongo'):
                mode = 'mongo'
                qq = QQ[mode]
        elif (db is not None or mode == 'sql'):
            if mode == 'mongo':
                t = DBS[mode](db = db, table = table, url = url or server, pk = pk)
                qq = QQ[mode]
            elif mode == 'sql':
                t = DBS[mode](db = db, table = table, server = server or url, schema = schema, pk = pk, doc = doc or _doc)
                qq = t.table.c
        else:
            return GRAPH[address]
        address = t.address + kwargs_address
        if _from_memory and deleted in (None, False): # we want the standard cell
            if address not in GRAPH:
                doc = _load_asof(t, kwargs, deleted, qq)
                if isinstance(doc, cell):
                    GRAPH[doc._address] = doc
                return doc
            else:
                return GRAPH[address]
        else:
            return _load_asof(t, kwargs, deleted, qq) # must not overwrite live version. User wants a specific deleted version
    else:
        return GRAPH[address]


def load_cell(table = None, db = None, url = None, server = None, deleted = None, doc = None, **kwargs):
    """
    retrieves a cell from a table in a database based on its key words. 
    In addition, can look at earlier versions using deleted.
    It is important to note that this DOES NOT go through the cache mechanism 
    but goes to the database directly every time.

    :Parameters:
    ----------
    table : str
        name of table (Mongo collection). alternatively, you can just provide an address
    db : str
        name of database.
    url : str, optional
        mongodb server location. The default is None.
    server : str or True, optional
        sql server location. The default is None (default to mongodb). Set to True to map to default sql server configured in cfg['sql_server']
    deleted : datetime/None, optional
        The date corresponding to the version of the cell we want
        None = live cell
        otherwise, the cell that was first deleted after this date.
    **kwargs : keywords
        key words for grabbing the cell.

    :Returns:
    ---------
    The document

    :Example:
    ---------
    >>> from pyg import *
    >>> people = partial(mongo_table, db = 'test', table = 'test', pk = ['name', 'surname'])
    >>> brown = db_cell(db = people, name = 'bob', surname = 'brown', age = 39).save()
    >>> assert load_cell('test','test', surname = 'brown').name == 'bob'
        
    """
    return _get_cell(table = table, db = db, url = url, deleted = deleted, server = server, doc = doc, **kwargs)

def get_docs(table = None, db = None, url = None, server = None, pk = None, cell = 'data', doc = None, **kwargs):
    """
    retrieves multiple cells from a table

    :Parameters:
    ----------
    table : str
        name of table (Mongo collection). alternatively, you can just provide an address
    db : str
        name of database.
    url : str, optional
        mongodb server location. The default is None.
    server : str or True, optional
        sql server location. The default is None (default to mongodb). Set to True to map to default sql server configured in cfg['sql_server']

    """
    schema = kwargs.pop('schema', None)
    mode = _get_mode(url, server, schema = schema)
    if mode == 'mongo':
        t = DBS[mode](db = db, table = table, url = url or server, pk = pk).inc(**kwargs)
    elif mode == 'sql':
        t = DBS[mode](db = db, table = table, schema = schema, server = server or url, pk = pk, doc = doc or _doc).inc(**kwargs)                
    return t.docs(list(kwargs.keys()))

    
def get_cell(table = None, db = None, url = None, server = None, deleted = None, doc = None, **kwargs):
    """
    unlike load_cell which will get the data from the database by default 
    get cell looks at the in-memory cache to see if the cell exists there.

    :Parameters:
    ----------
    table : str
        name of table (Mongo collection). alternatively, you can just provide an address
    db : str
        name of database.
    url : str, optional
        mongodb server location. The default is None.
    server : str or True, optional
        sql server location. The default is None (default to mongodb). Set to True to map to default sql server configured in cfg['sql_server']
    deleted : datetime/None, optional
        The date corresponding to the version of the cell we want
        None = live cell
        otherwise, the cell that was first deleted after this date.
    **kwargs : keywords
        key words for grabbing the cell.

    :Returns:
    -------
    The document

    :Example:
    ---------
    >>> from pyg import *
    >>> people = partial(mongo_table, db = 'test', table = 'test', pk = ['name', 'surname'])
    >>> brown = db_cell(db = people, name = 'bob', surname = 'brown', age = 39).save()
    >>> assert get_cell('test','test', surname = 'brown').name == 'bob'
    """
    _from_memory = kwargs.pop('_from_memory', True)
    params = dict(table = table, db = db, url = url, server = server, deleted = deleted)
    params.update(kwargs)
    multiples = {k : v for k, v in params.items() if isinstance(v, list)}
    if len(multiples) == 0:
        return _get_cell(_from_memory = _from_memory, doc = doc, **params)
    elif len(multiples) == 1:
        res = []
        for key, values in multiples.items():
            for value in values:
                params.update({key : value})
                res.append(_get_cell(_from_memory = _from_memory, doc = doc, **params))
        return res
    else:
        res = dictable(params)
        docs = []
        for row in res:
            params.update(row)
            docs.append(_get_cell(_from_memory = _from_memory, doc = doc, **params))
        res[doc or _doc] = docs
        return res

def get_data(table = None, db = None, url = None, server = None, deleted = None, doc = None, **kwargs):
    """
    retrieves a cell from a table in a database based on its key words. 
    In addition, can look at earlier versions using deleted.

    :Parameters:
    ----------
    table : str
        name of table (Mongo collection).
    db : str
        name of database.
    url : str, optional
        mongodb server location. The default is None.
    server : str or True, optional
        sql server location. The default is None (default to mongodb). Set to True to map to default sql server configured in cfg['sql_server']
    deleted : datetime/None, optional
        The date corresponding to the version of the cell we want
        None = live cell
        otherwise, the cell that was first deleted after this date.
    **kwargs : keywords
        key words for grabbing the cell.

    :Returns:
    -------
    The document

    :Example:
    ---------
    >>> from pyg import *
    >>> people = partial(mongo_table, db = 'test', table = 'test', pk = ['name', 'surname'])
    >>> people().reset.drop()
    >>> brown = db_cell(db = people, name = 'bob', surname = 'brown', age = 39).save()
    >>> assert get_data('test','test', surname = 'brown') is None
        
    """  
    cells = get_cell(table, db = db, url = url, server = server, deleted = deleted, doc = doc, **kwargs)
    if isinstance(cells, dictable):
        cells[doc or _doc] = cell_item(cells[doc or _doc], key = 'data')
        return cells
    else:
        return cell_item(cells, key = 'data')

def load_data(table = None, db = None, url = None, server = None, deleted = None, doc = None, **kwargs):
    """
    retrieves a cell from a table in a database based on its key words. 
    In addition, can look at earlier versions using deleted.

    :Parameters:
    ----------
    table : str
        name of table (Mongo collection).
    db : str
        name of database.
    url : str, optional
        mongodb server location. The default is None.
    server : str or True, optional
        sql server location. The default is None (default to mongodb). Set to True to map to default sql server configured in cfg['sql_server']
        
        
    deleted : datetime/None, optional
        The date corresponding to the version of the cell we want
        None = live cell
        otherwise, the cell that was first deleted after this date.
    **kwargs : keywords
        key words for grabbing the cell.

    :Returns:
    -------
    The document

    :Example:
    ---------
    >>> from pyg import *
    >>> people = partial(mongo_table, db = 'test', table = 'test', pk = ['name', 'surname'])
    >>> people().reset.drop()
    >>> brown = db_cell(db = people, name = 'bob', surname = 'brown', age = 39).save()
    >>> assert load_data('test','test', surname = 'brown') is None
        
    """    
    return cell_item(load_cell(table, db = db, url = url, server = server, deleted = deleted, doc = doc, **kwargs), key = 'data')

