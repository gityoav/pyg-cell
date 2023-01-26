# pyg-cell

pip install from https://pypi.org/project/pyg-cell/

## Introduction
pyg-cell is a light-weight framework for constructing calculation graphs in Python. 
There are a few open-source similar frameworks: Luigi (https://github.com/spotify/luigi), MDF (https://github.com/man-group/mdf)
and various financial variations such as JP Morgan Athena (https://www.youtube.com/watch?v=3ygPF-Qshkc) BAML's Quartz and Goldman's SecDB, the daddy of all calculation graphs. A version of these is available commercially by Beacon (https://www.beacon.io/)

If you have used Luigi, mdf or indeed any of the above, you will quickly see that pyg-cell is just much easier to use.

* incredibly flexible and yet entirely boiler-plate free. 
* supports functions that returns multiple values
* supports self-referencing in graphs: a cell that takes its output(s) as an input
* easy to persist, modify, extend, save in sql, mongo, parquet/npy/pickle files
* easy to control scheduling logic


    :Example: The cell
    ------------------------
```
    >>> from pyg_base import * 
    >>> from pyg_sql import * 
    >>> from pyg_cell import * 
    >>> import datetime
```

Here is a simple function:

```
    >>> def f(a, b):
    >>>     return a + b

    >>> def sum_a_list(values):
    >>>     return sum(values, 0)

    >>> c = f(a = 1, b = 2)
    >>> d = f(a = c, b = c)
    >>> e = f(a = c, b = d)
    >>> cde = sum_a_list(values = [c,d,e])
    >>> assert e == 9
    >>> assert cde == 18
```

The cell version looks like this:

```
    >>> c = cell(f, a = 1, b = 2)()  ##  we call the cell to evaluate it
    >>> d = cell(f, a = c, b = c)()
    >>> e = cell(f, a = c, b = d)()
    >>> cde = cell(sum_a_list, values = [c,d,e])()

    >>> assert e.data == 9
    >>> assert cde.data == 18

    >>> c

    cell
    a:
        1
    b:
        2
    function:
        <function f at 0x000002A74AE0DE50>
    data:
        3                               ## by default, output is in 'data' key
    latest:
        None                           
    updated:
        2023-01-24 18:26:04.951482     ## last calculated
```

    
Note that we didn't need to declare anything, nor to modify f to take a cell or a list of cells (or indeed a dict of cells) as an input etc. 
This is all done behind the scene to keep the API simple
    
### Example: The in-memory graph

There is no graph until we decide the primary keys we will use to save the cell in the graph.
Rather than pre-decide what the key is, cell allows you to specify your own primary keys.
You then you need to provide these keys, to ensure cell is registered with the graph:

```
    >>> c = cell(f, a = 1, b = 2, pk = 'key', key = 'c')()
    >>> d = cell(f, a = c, b = c, pk = ['name', 'surname'], name = 'james', surname = 'dean')()
    >>> e = cell(f, a = c, b = d, pk = ['exchange', 'stock', 'item'], exchange = 'NYSE', stock = 'AAPL', item = 'price')()
```
    
Here is what the log looks like:
    
```
    2023-01-25 15:33:19,302 - pyg - INFO - get_cell(key = 'c')()
    2023-01-25 15:33:19,311 - pyg - INFO - get_cell(name = 'james', surname = 'dean')()
    2023-01-25 15:33:19,320 - pyg - INFO - get_cell(exchange = 'NYSE', item = 'price', stock = 'AAPL')()  
```
    
In fact, the log is now executable:
    
```
    >>> get_cell(key = 'c')()
    cell
    a:
        1
    b:
        2
    pk:
        key
    key:
        c
    function:
        <function f at 0x000002A74AE0DE50>
    data:
        3
    latest:
        None
    updated:
        2023-01-24 18:34:47.983135
```
    
The data is also available based on the primary keys provided:

```
    >>> assert get_data(key = 'c') == 3
```



### Example: persistency in a database

The db_cell/document will be stored in a document store. You can choose either a MongoDB (using pyg_mongo) or MS SQL database (pyg_sql)


```
    >>> from pyg_sql import sql_table
    >>> server = 'DESKTOP-LU5C5QF'
    
    >>> db = partial(sql_table, server = server, db = 'test_db', table = 'people', schema = 'dbo', pk = ['name', 'surname'], doc = True) ## table is a doc-store
    >>> c = cell(f, a = 1, b = 2, pk = 'key', key = 'c')
    >>> d = db_cell(f, a = c, b = c, db = db, name = 'james', surname = 'dean')()
    
    2023-01-25 16:00:43,395 - pyg - INFO - creating table: test_db.dbo.people['name', 'surname', 'doc']
    2023-01-25 16:00:45,210 - pyg - INFO - get_cell(server = 'DESKTOP-LU5C5QF', db = 'test_db', schema = 'dbo', table = 'people', name = 'james', surname = 'dean')()
```


And indeed the data is now available in the table, either directly using get_cell/get_data:
    
```
    >>> get_data(server = 'DESKTOP-LU5C5QF', db = 'test_db', schema = 'dbo', table = 'people', name = 'james', surname = 'dean') == 6
    >>> assert db().inc(name = 'james', surname = 'dean')[0].data == 6
```


Note that when we access the document, we get back the cell object and all its functionality.
    
```
    >>> loaded_cell = db().inc(name = 'james', surname = 'dean')[0].go()

    2023-01-25 16:04:29,165 - pyg - INFO - get_cell(server = 'DESKTOP-LU5C5QF', db = 'test_db', schema = 'dbo', table = 'people', name = 'james', surname = 'dean')()
    2023-01-25 16:04:29,532 - pyg - INFO - creating table: test_db.archived_dbo.people['name', 'surname', 'doc', 'deleted']    
    
    >>> assert loaded_cell.data == 6
```

Two things to observe: 

* We decided that "c" as a cell, is not worth while saving in the database, that is fine and the graph can handle a mixture of persistent and transient cells: cell "d" that is persisted, will store internally the definition of c, and will calculate it every time it needs to calculate itself. 
* What is this new archived_dbo.people table? the cell manages persistency automatically, to ensure a full audit trail. The 'people' table has unique entries by name, surname, so the previous run of "d" is saved in the archived schema with an additional "deleted" primary key.


### Example: The cell writer, and complicated objects within a document

Suppose we have more complicated objects (usually pandas dataframes) as inputs. 
We can just save the actual data items in files:
    
```
    >>> db = partial(sql_table, server = server, 
            db = 'test_db', table = 'people', schema = 'dbo', 
            pk = ['name', 'surname'], doc = True,
            writer = 'c:/temp/%name/%surname.npy') ## please save numpy arrays/pandas in local file syste, using this location as "root" for the document

    >>> a = pd.Series([1,2,3])
    >>> b = pd.Series([4,5,6])
    >>> c = db_cell(f, a = a, b = b, db = db, name = 'adam', surname = 'smith')()
```
    
The document is saved in the sql people table, but the actual data is saved in the local files. Since there are three keys which are arrays/pandas, there will be three files:

```
    >>> import os
    >>> assert os.path.exists('c:/temp/adam/smith/data')
    >>> assert os.path.exists('c:/temp/adam/smith/a')
    >>> assert os.path.exists('c:/temp/adam/smith/b')
```

And the data can be read either via the cell interface:
    
```
    >>> get_cell(server = 'DESKTOP-LU5C5QF', db = 'test_db', schema = 'dbo', table = 'people', name = 'adam', surname = 'smith').data
    0    5
    1    7
    2    9
    dtype: int64
```
    
Or directly from file:
    
```
    >>> pd_read_npy('c:/temp/adam/smith/data')
    0    5
    1    7
    2    9
    Name: 0, dtype: int64

```
We support the following writers:

    * 'c:/temp/%name/%surname.npy': save both arrays and pandas as .npy files
    * 'c:/temp/%name/%surname.parquet': save pandas dataframe as .parquet file, numpy as npy
    * 'c:/temp/%name/%surname.pickle': save everything as .pickle files
    * 'server/db/schema/table/%surname/%name.sql' : pickle and save everything as binaries in a sql table.

Here is an example:
    
```
    >>> db = partial(sql_table, server = server, 
            db = 'test_db', table = 'people', schema = 'dbo', 
            pk = ['name', 'surname'], doc = True,
            writer = f'{server}/test_db/dbo/people_data/%name_%surname.sql') 

    >>> c = db_cell(f, a = a, b = b, db = db, name = 'winston', surname = 'smith')()
    
    2023-01-25 16:20:40,203 - pyg - INFO - get_cell(server = 'DESKTOP-LU5C5QF', db = 'test_db', schema = 'dbo', table = 'people', name = 'winston', surname = 'smith')()
    2023-01-25 16:20:40,433 - pyg - INFO - creating table: test_db.dbo.people_data['key', 'data', 'doc']
    
    >>> sql_table(server = server, db = 'test_db', table = 'people_data')
    
    sql_cursor: test_db.dbo.people_data DOCSTORE[doc] 
    SELECT key, data, doc 
    FROM dbo.people_data
    3 records

    >>> sql_table(server = server, db = 'test_db', table = 'people_data').key    
    ['winston_smith/a', 'winston_smith/b', 'winston_smith/data']
```


Each of the records contain a binary of the appropriate dataframe. 
The interface is unchanged though:

```
    >>> get_data(server = 'DESKTOP-LU5C5QF', db = 'test_db', schema = 'dbo', table = 'people', name = 'winston', surname = 'smith')
    Out[142]: 
    0    5
    1    7
    2    9
    dtype: int64
```


### Example: cells that use their outputs as an input

```
    >>> def daily_sample_prices(stock, data = None):
    >>>     today = dt(0)
    >>>     sampled_price = pd.Series(np.random.normal(0,1), [today])
    >>>     if data is None:
    >>>          return sampled_price
    >>>     elif data.index[-1] == today:
    >>>          return data
    >>>     else:
    >>>          return pd.concat([data, sampled_price])
```

Here, we deliberately used the existing output to feed back into the cell to decide if we want to resample:

```
    >>> c = cell(daily_sample_prices, stock = 'AAPL', pk= ['stock', 'item'], item = 'price')()

    2023-01-25 15:48:11,629 - pyg - INFO - get_cell(item = 'price', stock = 'AAPL')()

    >>> get_data(item = 'price', stock = 'AAPL')  
    >>> c.data    
    2023-01-25    0.228306
    dtype: float64
    
    >>> c.data = pd.Series(['old', 'data'], [dt(-2), dt(-1)])
    >>> c = c(); c.data    

    2023-01-25 15:49:52,968 - pyg - INFO - get_cell(item = 'price', stock = 'AAPL')()
    Out[107]: 
    2023-01-23         old
    2023-01-24        data
    2023-01-25   -1.694927  <--- added a new sample
    dtype: object

```

If you are a techie, your gut reaction is to complain: There is ambiguity and there is no "inputs" name space and "outputs" name space. 
This may cause confusion, but in practice allows us to switch from an "calculate everything historically" to "run online one at a time" with ease.
For example, if the calculation is very expensive, (e.g. some optimization result) being able to say "and here is the last time you ran it so run data only for today" is very useful indeed.

If you used e.g. mdf, and saw the slightly convoluted way you need to decorate all your functions to be able to handle this issue, you will appreciate the explicit way that we push the complexity to be handled internally only by specific functions that need this feature. 


## Calculation logic

The default cell, once calculated and persisted in memory, will only recalculate if its function inputs change.
The cell.run() method returns a boolean that determines if it needs running.

```
    >>> c = cell(f, a = 1, b = 2, pk = 'key', key = 'key')
    >>> assert c.run()
    >>> c = c()
    >>> assert not c.run()
    >>> c.some_parameter_unrelated_to_f = 0
    >>> assert not c.run()
    >>> c.a = 'some parameter'
    >>> c.b = 'that is important to f'
    >>> c = c() ## will recalculate
    >>> assert c.data == 'some parameterthat is important to f'
```
 
The data persists in the graph and we can now propagate both up and down the graph:

```
    >>> c.a = 3; c.b = 2
    >>> c = c.push()    
    2023-01-25 15:39:11,630 - pyg - INFO - get_cell(key = 'key')()
    2023-01-25 15:39:11,636 - pyg - INFO - get_cell(name = 'james', surname = 'dean')()
    2023-01-25 15:39:11,644 - pyg - INFO - get_cell(exchange = 'NYSE', item = 'price', stock = 'AAPL')()

    >>> assert get_data(exchange = 'NYSE', item = 'price', stock = 'AAPL') == 15
```

We can also force a recalculation by calling e.go() or e(go = 1)

```
    >>> e = e.go()

    2023-01-25 15:41:23,540 - pyg - INFO - get_cell(exchange = 'NYSE', item = 'price', stock = 'AAPL')()
```

### Changing the calculation scheduling
db_cell, once calculated, will not need to run again, unless we force it to recalculate, its inputs have changed or its output missing.
However, you may want to schedule periodical recalculation and this is extremely simple.
All you need to do is inherit from db_cell and implement a new run() method. For that reason, all db_cells have an "updated" key, about when it was last calculated and "latest" which is the latest index in cell.data if data is a timeseries.
    
Specifically, periodic_cell implements a simple scheduling mechanism so that periodic_cell(..., period = '1w') will re-calculate a week after the cell was last updated 
    
Interestingly, that's pretty much it. There is very little the user needs to do to. The framework:

    1) creates well-structured flow.
    2) has no boiler-plate
    3) saves and persists the data in nicely indexed tables with primary keys decided by use
    4) allows you to save the actual data either as files or within mongo/sql dbs as doc-stores.
    
