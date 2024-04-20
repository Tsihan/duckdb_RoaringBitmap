SELECT
  count(*)
FROM
    lineitem, orders
WHERE
   lineitem.L_LINESTATUS = orders.O_ORDERSTATUS
   AND lineitem.l_orderkey = orders.o_orderkey;
