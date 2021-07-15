select
uid
,PASStatusA24(paymt24)
from
(select
uid
,paymt24
from
(select "001" as uid, "*****NN**NNNNNNNNNNNNNN/" as paymt24
union all
select "001" as uid, "*****NN**NNNNNNNNNNNNNNA" as paymt24
union all
select "001" as uid, "*****NN**NNNNNNNNNNNNNNN" as paymt24
))t
group by t.uid