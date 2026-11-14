"""Arrival order is semantic; windows close against a monotonic watermark."""
def aggregate(events,width,delay,flush):
    opened={};closed=[];late=[];seen={};duplicates=0;maximum=None;watermark=None
    def close(reason):
        for start in sorted(list(opened)):
            if reason=='end_of_input' or start+width<=watermark:
                value=opened.pop(start);closed.append(dict(start=start,end=start+width,closed_by=reason,**value))
    for e in events:
        eid=e['id']
        if eid in seen:
            if seen[eid]!=e:raise ValueError('conflicting event ID')
            duplicates+=1;continue
        seen[eid]=e;start=e['event_time']//width*width
        if watermark is not None and start+width<=watermark:
            late.append(dict(e,reason='window_already_closed',watermark_at_arrival=watermark));continue
        window=opened.setdefault(start,{'count':0,'sum':0,'event_ids':[]})
        window['count']+=1;window['sum']+=e['value'];window['event_ids'].append(eid)
        maximum=e['event_time'] if maximum is None else max(maximum,e['event_time']);watermark=maximum-delay;close('watermark')
    if flush:close('end_of_input')
    pending=[dict(start=s,end=s+width,**v) for s,v in sorted(opened.items())]
    return {'closed_windows':closed,'pending_windows':pending,'late_events':late,'duplicate_events':duplicates,
            'max_event_time':maximum,'watermark':watermark,'accepted_count':sum(v['count'] for v in closed+pending),
            'accepted_sum':sum(v['sum'] for v in closed+pending)}
