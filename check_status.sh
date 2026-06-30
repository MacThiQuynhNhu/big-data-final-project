#!/bin/bash
echo '=== PostgreSQL cascade ==='
PGPASSWORD=erp123 psql -h localhost -U erp -d erp << 'SQL'
SELECT 'ngay' as c, COUNT(*) FROM agg_ngay
UNION ALL SELECT 'tuan', COUNT(*) FROM agg_tuan
UNION ALL SELECT 'thang', COUNT(*) FROM agg_thang
UNION ALL SELECT 'quy', COUNT(*) FROM agg_quy
UNION ALL SELECT 'nam', COUNT(*) FROM agg_nam ORDER BY 1;
SQL
echo '=== HDFS /lake ==='
/usr/local/hadoop/bin/hdfs dfs -count /lake/transactions/
/usr/local/hadoop/bin/hdfs dfs -count /lake/transactions_archive/
echo '=== Batch lock ==='
fuser /tmp/run_batch.lock 2>/dev/null && echo locked || echo free
