_db = 'db'
_root = 'root'
_writer = 'writer'
_table = 'table'
from pyg_base import dictattr, tree_setitem, get_cache, tree_repr
from pyg_encoders import root_path

def _doc_writer(doc):
    if _root in doc:
        return doc[_root]
    elif _db in doc:
        db = doc[_db]
        return db.keywords.get(_writer)
 
def _address_key(address):
    k,v = zip(*address)
    return k

def _doc_key(doc, address = None):
    address = address or doc._address
    if _db in doc:
        pk = doc._pk
        return tuple([pair[0] if pair[0] in pk else pair[1] for pair in address])
    else:
        return _address_key(doc._address)

def _doc_write(doc, writer, tree):
    path = root_path(doc, writer).lower().replace(':', '').replace(' ', '_')
    nodes = path.split('/')
    nodes[-1] = '.'.join(nodes[-1].split('.')[:-1])
    tree_setitem(tree, nodes, doc)
    

def cell_tree(docs = None, tree = None, skip = True, mappers = None):
    ## This creates a graph based on the document primary keys
    """
    from pyg import  * 
    ticker = mongo_table('ticker', 'momentum')
    tdocs = ticker.inc(item = ['buffered_signal', 'uncertainty', 'combined_signal']).docs()
    tdocs = tdocs.do(cell_go)
    
    predictor = mongo_table('predictor', 'momentum')
    pdocs = predictor.inc(predictor = ['mom_1', 'mom_2']).docs()
    
    >>> tree = cell_tree()
    >>> tree = cell_tree(mappers = {('localhost:27017', 'cme', 'expiry', 'ticker'): 'C:/Users/Dell/Dropbox/Yoav/TradingData/bbg/ticker/%ticker/%expiry.parquet', 
                                    ('localhost:27017', 'ice', 'expiry', 'ticker'): 'C:/Users/Dell/Dropbox/Yoav/TradingData/bbg/ticker/%ticker/%expiry.parquet'})
    >>> tree = cell_tree(docs.doc + pdocs.doc + [cell(pk = ['a'], a = 1)])
    """
    missing = {}
    really_missing = {}
    mappers = mappers or {}
    writers = {}
    tree = tree or dictattr()
    if docs is None:
        docs = get_cache()['GRAPH']
    if isinstance(docs, list):
        docs = {doc._address : doc for doc in docs}
    for address, doc in docs.items():
        key = _doc_key(doc, address)
        if key in mappers:
            writer = mappers[key]
        else:
            writer = _doc_writer(doc)
            if writer is not None:
                writers[key] = writer
            elif key in writers:
                writer = writers[key]
            else:
                missing[address] = doc
        if writer is not None:
            _doc_write(doc, writer, tree)
    for address, doc in missing.items():
        key = _doc_key(doc, address)
        if key not in writers:
            if not skip:
                raise ValueError('not sure how to add this key %s'%key)
            else:
                really_missing[key] = address 
        else:
            writer = writers[key]
            _doc_write(doc, writer, tree)
    while isinstance(tree, dict) and len(tree) == 1:
        tree = list(tree.values())[0]
    if really_missing:
        print('we were not able to map these keys into a logical place within the graph', really_missing.keys())
        print('these got mapped:\n', tree_repr(writers))
    return tree
                
            

    
    
    
    
    
    