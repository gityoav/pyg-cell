## Here is where we resolve SQL vs Mongo issues

DBS = {}
QQ = {}
CLSS = {}

try:
    from pyg_mongo import q, mongo_base_reader, mongo_table
    DBS['mongo'] = mongo_table
    QQ['mongo'] = q
    CLSS['mongo'] = mongo_base_reader
except Exception:
    pass

try:
    from pyg_sql import sql_cursor, sql_table
    DBS['sql'] = sql_table
    CLSS['sql'] = sql_cursor
except Exception:
    pass

if len(DBS) == 0:
    raise ValueError('pyg-cell can work with either/both of: pyg-sql and/or pyg-mongo but we need at least ONE of them. Please install either packages to proceed')

def _get_mode(url, server, schema = None):
    if 'mongo' not in DBS:
        return 'sql'
    elif 'sql' not in DBS:
        return 'mongo'
    elif schema is not None:
        return 'sql'
    elif server is None:
        return 'mongo'
    else:
        return 'sql'
    
    
def _get_qq(qq, table):
    if qq is None:
        if 'mongo' in CLSS and isinstance(table, CLSS['mongo']):
            qq = QQ['mongo']
        elif 'sql' in CLSS and isinstance(table, CLSS['sql']):
            qq = table.table.c
        else:
            raise ValueError('need to specify query mechanism')
    else:
        return qq
