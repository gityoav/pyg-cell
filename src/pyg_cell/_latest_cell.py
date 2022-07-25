from pyg_cell._cell import cell_inputs
from pyg_cell._periodic_cell import periodic_cell

__all__ = ['latest_cell']
_latest = 'latest'

class latest_cell(periodic_cell):
    """
    latest_cell 
    - inherits from periodice_cell
    - Its output MUST contain a timeseries
    - If my inputs have outputs which have timeseries data PAST my own latest data, I run().    

    :Example:
    ---------
    >>> from pyg import * 
    >>> a = db_cell(passthru, data = pd.Series(np.random.normal(0,1,100), drange(99)), pk = 'key', key = 'a')
    >>> b = latest_cell(ewma, a = a , n = 10, pk = 'key', key = 'b')
    >>> c = latest_cell(ewma, a = b, n = 5, pk = 'key', key = 'c')
    >>> c = c()
    >>> assert not c.run()    

    ## update a to have one more datapoint
    
    >>> a.data = pd.Series(np.random.normal(0,1,101), drange(100)) # add a data point
    >>> a = a.go() ## need to register the change in the graph

    >>> assert b.run()
    >>> assert c.run()
    >>> c = c()
    """         
    def run(self):
        latest = self._latest
        if latest is None:
            return True
        inputs = cell_inputs(self._inputs)
        for i in inputs:
            i = i.load()
            i_latest = i._latest
            if i_latest is not None and i_latest > latest:
                return True
        return super(latest_cell, self).run()


