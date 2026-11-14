from .common import require,ident,number,rows
from .windows import aggregate

def analyze(data):
    require(data,'window_seconds out_of_order_seconds flush events');width=number(data['window_seconds'],1,86400)
    delay=number(data['out_of_order_seconds'],0,86400)
    if type(data['flush']) is not bool:raise ValueError('flush must be boolean')
    events=[]
    for e in rows(data['events'],20000):
        require(e,'id event_time value');ident(e['id']);number(e['event_time'],0,10**12);number(e['value'],-10**9,10**9);events.append(e)
    return aggregate(events,width,delay,data['flush'])
