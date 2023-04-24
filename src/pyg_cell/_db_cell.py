from pyg_base import dt, eq, is_date, ulist, is_str, is_strs, as_list, get_cache, Dict, dictable, list_instances
from pyg_encoders import cell_root, root_path, pd_read_parquet, pickle_load, pd_read_csv, dictable_decode
from pyg_npy import pd_read_npy
from pyg_cell._types import _get_mode, _get_qq, DBS, QQ, CLSS
from pyg_cell._cell import cell, cell_clear, cell_item, cell_output, cell_func
from pyg_cell._dag import get_DAG, get_GAD, descendants

from functools import partial
# import networkx as nx
import os

def is_partial(db):
    return isinstance(db, partial)

def not_partial(db):
    return not isinstance(db, partial)

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
_writer = 'writer'


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
    """
    loads the data asof a particular date in deleted.

    Logic:    
    ------
    Our base table is table.inc(kwargs)
        
    if deleted is False: 
        we ignore the deleted table completely, returning just the live document
    if deleted is True:
        we ignore the live table and return the simulation last deleted document
    if deleted is None:
        we return the live document if available. if not, the last deleted document
    if deleted is a date:
        We look at documents deleted AFTER deleted date. 
        if there are such documents:
            we return the earliest of those, assuming it was alive at deleted
        if there are no such documents, we return the live document if available
        
    """
    live = table.inc(kwargs)
    l = len(live)
    if deleted is False: # we just want live values
        if l == 1:
            return live[0]
        elif l>1:
            raise ValueError(f'multiple cells found in \n{live}')
        elif l == 0:
            raise ValueError(f'no live cells found in \n{live}')
    past = table.deleted.inc(kwargs)
    if deleted is True: ## we want the latest deleted
        if len(past):
            return past.sort('deleted')[-1]
        else:
            raise ValueError(f'no deleted cells found in \n{past}')
    if deleted is None:  
        if l == 1:
            return live[0]
        elif l>1:
            raise ValueError(f'multiple cells found in \n{live}')
        elif len(past): ## return latest deleted
            return past.sort('deleted')[-1]
        else:
            raise ValueError(f'no live cells found in \n{live}\nnor deleted cells found in \n{past}')
    
    if qq is None:
        past = past.inc(past.c.deleted > dt(deleted))
    else:
        past = past.inc(qq['deleted'] > dt(deleted))

    if len(past):
        return past.sort('deleted')[0] 
    elif l == 1:
        return live[0]
    elif l>1:
        raise ValueError(f'multiple cells found in \n{live}')
    elif l == 0:
        raise ValueError(f'no live cells found in \n{live}\nnor deleted cells found in \n{past}')


import datetime
def _is_primitive(value):
    if isinstance(value, (tuple, list)):
        return min([_is_primitive(v) for v in value], default = True)
    elif isinstance(value, dict):
        return min([_is_primitive(v) for v in value.values()], default = True)
    else:
        return value is None or isinstance(value, (type, bool, str, int, float, datetime.datetime, datetime.date, partial, cell))

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
            if is_partial(db):
                non_primitives = {key : value for key, value in db.keywords.items() if not _is_primitive(value)}
                if len(non_primitives):
                    raise ValueError('partial construction of cell must be from primitive paramters. but these are not: %s'%non_primitives)
            super(db_cell, self).__init__(function = function, output = output, db = db, **kwargs)
        else:
            self[_db] = None
            super(db_cell, self).__init__(function = function, output = output, **kwargs)

    _func = db_cell_func
    _shared_resources = ['engine', 'session', 'connection']

    @property
    def _function(self):
        db = self.get(_db)
        if not_partial(db):
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
        db = self.get(_db)
        if not_partial(db):
            return super(db_cell, self)._pk
        return self.db.keywords.get(_pk)
    
    def _db(self, **kwargs):
        """
        equivalent to self.db() but allows the user to 
        1) pass into partial function cells 
        2) pass pointers to complicated elements within the cell
    
        """
        value = cell_item(self.get(_db))
        if is_partial(value):
            keywords = {k: self.get(v, v) if is_str(v) and k in self._shared_resources and not is_str(self.get(v, v)) else v for k, v in value.keywords.items()}
            keywords = {k: cell_item(v()) if isinstance(v, cell) else v for k, v in keywords.items()}
            args = value.args
            func = value.func
            func = cell_item(func()) if isinstance(func, cell) else func
            keywords.update(kwargs)
            return func(*args, **keywords)
        else:
            return self.db
            

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
        db = self.get(_db)
        if not_partial(db):
            return super(db_cell, self)._address        
        db = db()
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
        if not_partial(self.get(_db)): 
            return super(db_cell, self)._clear()
        else:
            pk = self._db()._pk
            return self[[_db, _function, _updated] + pk] if _updated in self else self[[_db, _function] + pk]

    def save(self):
        db = self.get(_db)
        if not_partial(db):
            return super(db_cell, self).save()
        address = self._address
        doc = (self - _deleted)
        db = self._db()
        missing = ulist(db._pk) - self.keys()
        if len(missing):
            self._logger.warning('WARN: document not saved as some keys are missing %s'%missing)
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
        
        Example:Persistency:
        -------------
        Since we want to avoid hitting the database, there is a singleton GRAPH, a dict, storing the cells by their address.
        Every time we load/save from/to Mongo, we also update GRAPH.
        
        We use the GRAPH often so if you want to FORCE the cell to go to the database when loading, use this:

        >>> cell.load(-1) 
        >>> cell.load(-1).load(0)  # clear GRAPH and load from db
        >>> cell.load([0])     # same thing: clear GRAPH and then load if available

        Example:forcing update on change in function inputs:
        ---------------------------------------------
        If the current cell, has inputs that is different from the saved document inputs, we force a recalculation by setting updated = None

        >>> from pyg import *
        >>> db = partial(sql_table, server = 'DESKTOP-LU5C5QF', table = 'test', db = 'test_db', schema = 'dbo', pk = 'key', doc =True)
        >>> doc = db_cell(add_, a = 1, b = 2, c = 3, key = 'a', db = db)()
        >>> same_doc = db_cell(db = db, key = 'a').load()
        >>> assert same_doc.run() is False
        >>> new = db_cell(add_, a = 2, b = 2, c = 3, key = 'a', db = db).load()
        >>> assert new.run() is True
        >>> assert db_cell(add_, a = 2, b = 2, c = 3, key = 'a', db = db)().data == 4
        
        Example:Merge of cached cell and calling cell:
        ----------------------------------------
        Once we load from memory (either MongoDB or GRAPH), we tree_update the cached cell with the new values in the current cell.
        This means that it is next to impossible to actually *delete* keys. If you want to delete keys in a cell/cells in the database, you need to:
        
        >>> del db.inc(filters)['key.subkey']

        :Parameters:
        ----------
        mode : int , dataetime, str(s), optional
            if -1,   then will not load and clears the GRAPH
            if 0,    then will load from database if found. If not found, will return original document
            if 1,    then will load, if doc not found, data from files (equivalent to db_cell.load_output)
            if date, then will return the version alive at that date 
            if strs, then will exclue these keys from the keys loaded from old document
            The default is 0.
            
    
        :Returns:
        -------
        document

        """
        if not_partial(self.get(_db)):
            return super(db_cell, self).load(mode = mode)
        # if isinstance(mode, (list, tuple)):
        #     if len(mode) == 0:
        #         return self.load()
        #     if len(mode) == 1:
        #         return self.load(mode[0])
        #     if len(mode) == 2 and mode[0] == -1:
        #         res = self.load(-1)
        #         return res.load(mode[-1])
        #     else:
        #         raise ValueError('mode can only be of the form [], [mode] or [-1, mode]')
        db = self._db(mode = 'w')
        pk = ulist(db._pk)
        missing = pk - self.keys()
        if len(missing):
            self._logger.warning('WARN: document not loaded as some keys are missing %s'%missing)
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
                        return self.load_output() # just load outputs if exists
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
            if is_strs(mode):
                excluded_keys += as_list(mode)
            elif is_date(mode):
                excluded_keys += [_id]
            update = (saved / None) - excluded_keys
            updated_inputs = [k for k, v in self._inputs.items() if k in saved and v is not None and not isinstance(v, cell) and not eq(saved[k], v)]
            self.update(update)
            if len(updated_inputs):
                self[_updated] = None
        return self        


    def run(self):
        """
        Checks to see if any of the cells in the inputs have been updated since self has run.

        Example
        --------
        >>> from pyg import *
        >>> a = db_cell(passthru, data = 1, db = 'key', key = 'a')()
        >>> b = db_cell(passthru, data = 2, db = 'key', key = 'b')()
        >>> c = db_cell(add_, a = a, b = b, db = 'key', key = 'c')()
    
        ### suppose we now ran a        
        >>> a = a(data = 3).go()

        ### and a little later we loaded c
        >>> c = db_cell(add_, a = a, b = b, db = 'key', key = 'c').load()
        >>> assert c.updated < c.a.updated
        >>> assert c.run()
        
        """
        updated = self.get(_updated)
        if updated is None:
            return True
        input_cells = list_instances(self._inputs, cell)
        for c in input_cells:
            u = c.get(_updated)
            if is_date(u) and u > updated:
                return True
        return super(db_cell, self).run()


    def load_output(self, writer = None):
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
        writer = writer or self.get(_writer)
        for output in cell_output(self):
            for suffix, reader in _readers.items():
                filename = path[:-n] + '/%s.%s'%(output, suffix)            
                if os.path.exists(filename):
                    self[output] = reader(filename)
                elif suffix == 'sql' and is_str(writer) and writer.endswith('sql'):
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
        
class calculating_cell(db_cell):
    """
    This is a cell that when saved in the database, is calculated without its output.
    """
    def save(self):
        output_keys = self._output
        output = cell_item(self)
        doc = db_cell(self - output_keys)
        res = type(self)(doc.save())
        if isinstance(output, dict) and len(output_keys>1):
            res.update(output)
        else:
            res[output_keys[0]] = output
        address = res._address
        get_GRAPH()[address] = res
        return res
        


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



def _get_cell(table = None, db = None, url = None, schema = None, server = None, deleted = False, doc = None, _use_graph = True, **kwargs):
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
    deleted : datetime/None/False, optional
        The date corresponding to the version of the cell we want
        False = only live cell
        
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
                                                         schema = schema, 
                                                         deleted = deleted,
                                                         ).items() if value is not None})
        params.update(kwargs)
        return _get_cell(**params)
    
    pk = kwargs.pop('pk', None)
    if pk is None:
        if isinstance(table, partial):
            pk = as_list(table().pk)
        elif isinstance(table, tuple(CLSS.values())):
            pk = as_list(table.pk)
        elif hasattr(table, 'pk'):
            pk = as_list(table.pk)
    if pk is None:
        address = kwargs_address = tuple(sorted(kwargs.items()))
    else:
        pk = sorted(as_list(pk))
        address = kwargs_address = tuple([(key, kwargs.get(key)) for key in pk]) 
    
    mode = _get_mode(url, server, schema = schema) ## which database
    if table is not None:
        if is_partial(table):
            t = table()
            if table.func == DBS.get('sql'):
                mode = 'sql'
                qq = None
            elif table.func == DBS.get('mongo'):
                mode = 'mongo'
                qq = QQ[mode]
        elif 'sql' in CLSS and isinstance(table, CLSS['sql']):
            t = table
            mode = 'sql'
            qq = None
        elif 'mongo' in CLSS and isinstance(table, CLSS['mongo']):
            t = table
            mode = 'mongo'
            qq = QQ[mode]
        elif (db is not None or mode == 'sql'):
            if mode == 'mongo':
                t = DBS[mode](db = db, table = table, url = url or server, pk = pk)
                qq = QQ[mode]
            elif mode == 'sql':
                t = DBS[mode](db = db, table = table, server = server or url, schema = schema, pk = pk, doc = doc or _doc)
                qq = None
        else:
            if _use_graph:
                return GRAPH[address].copy()
        address = t.address + kwargs_address
        if deleted is False or deleted is None: 
            if _use_graph and address in GRAPH:
                return GRAPH[address].copy()
            else:
                doc = _load_asof(table = t, kwargs = kwargs, deleted = deleted, qq = qq)
                if isinstance(doc, cell) and _use_graph:
                    GRAPH[doc._address] = doc
                return doc.copy()
        else:
            return _load_asof(table = t, kwargs = kwargs, deleted = deleted, qq = qq) # must not overwrite live version. User wants a specific deleted version
    else:
        return GRAPH[address].copy()


def load_cell(table = None, db = None, url = None, schema = None, server = None, deleted = False, doc = None, **kwargs):
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
    deleted : datetime/None/True/False, optional.
        There are multiple documents sources available:
            - the one in the live table
            - the ones in the deleted table.
    
        We use "deleted" parameter to choose between the documents that are available.

        The logic we follow is this:

        if deleted is False: (default)
            we ignore the deleted table completely, returning just the live document from database, otherwise we throw.
        if deleted is None:
            we return the document in live table if available. if not, the last deleted document
        if deleted is True:
            we ignore the live table and the graph and return the last deleted document
        if deleted is a date:
            We look at documents deleted AFTER deleted date. 
            if there are such documents:
                we return the earliest of those, assuming it was alive at deleted
            if there are no such documents, we return the document in live table if available
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
    return _get_cell(table = table, db = db, url = url, schema = schema, deleted = deleted, server = server, doc = doc, _use_graph = False, **kwargs)

def get_docs(table = None, db = None, url = None, schema = None, server = None, pk = None, cell = 'data', doc = None, **kwargs):
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
    mode = _get_mode(url, server, schema = schema)
    if mode == 'mongo':
        t = DBS[mode](db = db, table = table, url = url or server, pk = pk).inc(**kwargs)
    elif mode == 'sql':
        t = DBS[mode](db = db, table = table, schema = schema, server = server or url, pk = pk, doc = doc or _doc).inc(**kwargs)                
    return t.docs(list(kwargs.keys()))

    
def get_cell(table = None, db = None, schema = None, url = None, server = None, deleted = False, doc = None, **kwargs):
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
    
    deleted : datetime/None/True/False, optional.
        There are possible multiple documents available:
            - the in-memory one, in the graph
            - the one in the live table, presumably an earlier version
            - the ones in the deleted table.
    
        We use "deleted" to choose between the documents that are available.

        The logic we follow is this:

        if deleted is False: (default)
            we ignore the deleted table completely, returning just the live document from graph or database, otherwise we throw.
        if deleted is None:
            we return the graph if available, document in live table if available. if not, the last deleted document
        if deleted is True:
            we ignore the live table and the graph and return the last deleted document
        if deleted is a date:
            We look at documents deleted AFTER deleted date. 
            if there are such documents:
                we return the earliest of those, assuming it was alive at deleted
            if there are no such documents, we return the document in live table if available

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
    params = dict(table = table, db = db, url = url, server = server, deleted = deleted, schema = schema)
    params.update(kwargs)
    multiples = {k : v for k, v in params.items() if isinstance(v, list)}
    if len(multiples) == 0:
        return _get_cell(doc = doc, **params)
    elif len(multiples) == 1:
        res = []
        for key, values in multiples.items():
            for value in values:
                params.update({key : value})
                res.append(_get_cell(doc = doc, **params))
        return res
    else:
        res = dictable(params)
        docs = []
        for row in res:
            params.update(row)
            docs.append(_get_cell(doc = doc, **params))
        res[doc or _doc] = docs
        return res


def get_data(table = None, db = None, url = None, schema = None, server = None, deleted = False, doc = None, **kwargs):
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
    deleted : datetime/None/True/False, optional.
        There are possible multiple documents available:
            - the in-memory one, in the graph
            - the one in the live table, presumably an earlier version
            - the ones in the deleted table.
    
        We use "deleted" to choose between the documents that are available.

        The logic we follow is this:

        if deleted is False: (default)
            we ignore the deleted table completely, returning just the live document from graph or database, otherwise we throw.
        if deleted is None:
            we return the graph if available, document in live table if available. if not, the last deleted document
        if deleted is True:
            we ignore the live table and the graph and return the last deleted document
        if deleted is a date:
            We look at documents deleted AFTER deleted date. 
            if there are such documents:
                we return the earliest of those, assuming it was alive at deleted
            if there are no such documents, we return the document in live table if available

    **kwargs : keywords
        key words for grabbing the cell.

    :Returns:
    -------
    The document data

    :Example:
    ---------
    >>> from pyg import *
    >>> people = partial(mongo_table, db = 'test', table = 'test', pk = ['name', 'surname'])
    >>> people().reset.drop()
    >>> brown = db_cell(db = people, name = 'bob', surname = 'brown', age = 39).save()
    >>> assert get_data('test','test', surname = 'brown') is None
        
    """  
    cells = get_cell(table, db = db, url = url, schema = schema, server = server, deleted = deleted, doc = doc, **kwargs)
    if isinstance(cells, dictable):
        cells[doc or _doc] = cell_item(cells[doc or _doc], key = 'data')
        return cells
    else:
        return cell_item(cells, key = 'data')


def load_data(table = None, db = None, url = None, schema = None, server = None, deleted = False, doc = None, **kwargs):
    """
    retrieves a cell from a table in a database based on its key words, ignoring the in-memory graph document 
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

    deleted : datetime/None/True/False, optional.
        There are multiple documents sources available:
            - the one in the live table
            - the ones in the deleted table.
    
        We use "deleted" parameter to choose between the documents that are available.

        The logic we follow is this:

        if deleted is False: (default)
            we ignore the deleted table completely, returning just the live document from database, otherwise we throw.
        if deleted is None:
            we return the document in live table if available. if not, the last deleted document
        if deleted is True:
            we ignore the live table and the graph and return the last deleted document
        if deleted is a date:
            We look at documents deleted AFTER deleted date. 
            if there are such documents:
                we return the earliest of those, assuming it was alive at deleted
            if there are no such documents, we return the document in live table if available

    **kwargs : keywords
        key words for grabbing the cell.

    :Returns:
    -------
    The document data

    :Example:
    ---------
    >>> from pyg import *
    >>> people = partial(mongo_table, db = 'test', table = 'test', pk = ['name', 'surname'])
    >>> people().reset.drop()
    >>> brown = db_cell(db = people, name = 'bob', surname = 'brown', age = 39).save()
    >>> assert load_data('test','test', surname = 'brown') is None
        
    """    
    return cell_item(load_cell(table, db = db, url = url, schema = schema, server = server, deleted = deleted, doc = doc, **kwargs), key = 'data')



