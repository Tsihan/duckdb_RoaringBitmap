Q:CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER);
Q:INSERT INTO t1(e,b,a,c,d) VALUES(237,236,239,NULL,238);
Q:SELECT d,       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),       abs(b-c),       c-d,       CASE a+1 WHEN b THEN 111 WHEN c THEN 222        WHEN d THEN 333  WHEN e THEN 444 ELSE 555 END,       a+b*2,       b-c  FROM t1 WHERE b>c   AND (e>c OR e<d)
128|3|4|-3|333|385|4
136|5|2|1|111|416|2
140|6|2|1|111|428|2
|9|3||555|475|3
196|17|3|-1|555|595|3
217|20|3|-2|333|652|3
226|22|3|-1|555|685|3
233|23|1|-2|555|698|1
248|26|2|-1|444|743|2
