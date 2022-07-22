from pyg_base import cell_inputs
from pyg_cell._db_cell import db_cell
from pyg_cell._periodic_cell import periodic_cell

__all__ = ['latest_cell']
_latest = 'latest'

class latest_cell(db_cell):
    """
    latest_cell 
    - inherits from periodice_cell
    - Its output is a timeseries of some sort
    - If the inputs have outputs which have latest data PAST my own latest data, I recalculate.    

    >>> from pyg import * 
    >>> a = db_cell(passthru, data = pd.Series(np.random.normal(0,1,100), drange(99)), pk = 'key', key = 'a')
    >>> b = latest_cell(ewma, a = a , n = 10, pk = 'key', key = 'b')
    >>> c = latest_cell(ewma, a = b, n = 5, pk = 'key', key = 'c')
    >>> c = c()
    >>> assert not c.run()    

    ## update a to have one more datapoint
    
    >>> a.data = pd.Series(np.random.normal(0,1,101), drange(100)) # add a data point
    >>> a = a.go()

    >>> assert b.run()
    >>> assert c.run()    
    """         
    def run(self):
        latest = self._latest
        if latest is None:
            return True
        inputs = cell_inputs(self._inputs)
        for i in inputs:
            i = i.load()
            if i.run():
                return True
            i_latest = i._latest
            if i_latest is not None and i_latest > latest:
                return True
        return super(latest_cell, self).run()


