#Option 1:

sqoop import --connect jdbc:mysql://127.0.0.1:3306/bigdata2020 --table emp --username test1 --password Abcd1234! --target-dir /sqoop_out_emp  -m 1
sqoop import --connect jdbc:mysql://127.0.0.1:3306/bigdata2020 --table department --username test1 --password Abcd1234! --target-dir /sqoop_out_department  -m 1


#Option 2:
sqoop import-all-tables --connect jdbc:mysql://127.0.0.1:3306/bigdata2020 --username test1 --password Abcd1234! --warehouse-dir /sqoop  -m 1
