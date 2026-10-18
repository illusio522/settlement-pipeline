from copy import deepcopy
from datetime import datetime, date
from decimal import Decimal
import json
from pathlib import Path

from file_batch.bundle import BundleWriter

DAY='2026-05-20'

def data():
    pays=[dict(txn_id=100,order_no=10,brand=1,amount=100,type=1,orig_txn_id=None,event_time=datetime(2026,5,19,12)),
          dict(txn_id=200,order_no=20,brand=1,amount=101,type=1,orig_txn_id=None,event_time=datetime(2026,5,20,10)),
          dict(txn_id=900,order_no=10,brand=1,amount=33,type=2,orig_txn_id=100,event_time=datetime(2026,5,20,11))]
    def method(pm,tx,order,code,mid,amt,when):
        return dict(pm_id=pm,txn_id=tx,order_no=order,method=code,payments_mid=mid,amount=amt,type=1,event_time=when)
    methods=[method(1001,100,10,1,1001,60,pays[0]['event_time']),
             method(1002,100,10,2,1101,40,pays[0]['event_time']),
             method(2001,200,20,1,1001,86,pays[1]['event_time']),
             method(2003,200,20,3,None,15,pays[1]['event_time'])]
    items=[dict(order_no=order,line_no=line,partner_id=partner,brand=1,item_amount=amt)
           for order,line,partner,amt in [(10,1,1,33),(10,2,2,33),(10,3,1,34),(20,1,1,50),(20,2,2,51)]]
    return dict(payment=pays,payment_method=methods,order_item=items,
                cancel=[dict(cancel_txn_id=900,order_no=10,line_no=1)],
                partner=[dict(partner_id=1,name='seller1',brand=1),dict(partner_id=2,name='seller2',brand=1)],
                method_fee=[dict(method=m,fee_rate=Decimal(rate),effective_from=date(2020,1,1))
                            for m,rate in [(1,'0.0250'),(2,'0.0150'),(3,'0.0000'),(4,'0.0000')]]+
                           [dict(method=1,fee_rate=Decimal('0.0230'),effective_from=date(2026,5,20))],
                mid_map=[dict(payments_mid=1001,brand=1),dict(payments_mid=1101,brand=1)])


def close_for(source,day=DAY):
    pays=[p for p in source['payment'] if str(p['event_time'].date())==day]
    return dict(ready=True,reference='synthetic-source-complete',settlement_date=day,expected={
        'pay_count':sum(p['type']==1 for p in pays),'cancel_count':sum(p['type']==2 for p in pays),
        'gross':sum(p['amount'] for p in pays if p['type']==1),
        'cancel':sum(p['amount'] for p in pays if p['type']==2)})


def bundle(root,source=None,day=DAY):
    source=source or data()
    target=[p for p in source['payment'] if str(p['event_time'].date())==day]
    ids={p['txn_id'] for p in target}
    originals={p['orig_txn_id'] for p in target if p['type']==2}
    orders={p['order_no'] for p in target}
    tables={
        'payment':target,
        'payment_method':[m for m in source['payment_method'] if m['txn_id'] in ids],
        'original_payment':[p for p in source['payment'] if p['txn_id'] in originals],
        'original_payment_method':[m for m in source['payment_method'] if m['txn_id'] in originals],
        'order_item':[o for o in source['order_item'] if o['order_no'] in orders],
        'cancel':[c for c in source['cancel'] if c['cancel_txn_id'] in ids],
        **{t:source[t] for t in ('partner','method_fee','mid_map')}}
    writer=BundleWriter(root,day,close_for(source,day),'fixture-v1')
    for name,rows in tables.items():
        writer.write(name,[rows])
    return writer.finish()


def master_json(path,source):
    value={'version':'fixture-v1',**{t:source[t] for t in ('partner','method_fee','mid_map')}}
    Path(path).write_text(json.dumps(value,default=str))
