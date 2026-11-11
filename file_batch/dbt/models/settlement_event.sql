SELECT brand||':'||txn_id||':'||line_no||':'||method AS event_id,
    txn_id,order_no,line_no,partner_id,brand,type,method,payments_mid,
    method_alloc AS allocated_amount,fee_rate,fee,
    method_alloc-fee AS net_amount,event_time,settlement_date
FROM {{ ref('int_priced') }}
