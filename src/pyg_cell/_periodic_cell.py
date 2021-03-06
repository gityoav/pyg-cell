from pyg_base import dt, dt_bump, calendar, cfg_read, is_date
from pyg_cell._db_cell import db_cell, _updated

__all__ = ['periodic_cell']
_period = 'period'

_day_start = cfg_read().get('day_start', 0)
_day_end = cfg_read().get('day_end', 235959)

class periodic_cell(db_cell):
    """
    periodic_cell inherits from db_cell its ability to save itself in MongoDb using its db members
    Its calculation schedule depends on when it was last updated. 
    
    :Example:
    ---------
    >>> from pyg import *
    >>> c = periodic_cell(lambda a: a + 1, a = 0)
    
    We now assert it needs to be calculated and calculate it...

    >>> assert c.run()
    >>> c = c.go()
    >>> assert c.data == 1
    >>> assert not c.run()
    
    Now let us cheat and tell it, it was last run 3 days ago...
    
    >>> c.updated = dt(-3)
    >>> assert c.run()
    
    self = c    
    
            
    """
    def __init__(self, function = None, output = None, db = None, period = '1b', updated = None, **kwargs):
        self[_updated] = updated
        self[_period] = period 
        super(periodic_cell, self).__init__(function, output = output, db = db, **kwargs)
            
    def run(self):
        end_date = self.get('end_date')
        time = dt()
        updated = self.get(_updated)
        # if self[_updated] is None or dt_bump(self[_updated], self[_period]) < time:
        if is_date(end_date) and time > dt(end_date, 1) and updated is not None and dt_bump(updated, self[_period]) >= end_date: ## we expired and last ran near expiry
            return super(periodic_cell, self).run() 
        elif updated is None:
            return True
        else:
            cal = calendar(self.get('calendar'))
            next_update = cal.dt_bump(updated, self.period)
            day_start = self.get('day_start', _day_start)
            day_end = self.get('day_end', _day_end)                            
            if cal.trade_date(time, 'p', day_start, day_end) >= cal.trade_date(next_update, 'f', day_start, day_end):
                return True
            else:
                return super(periodic_cell, self).run() 


    
