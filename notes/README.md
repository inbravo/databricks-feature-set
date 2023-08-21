## Notes for Databricks Certified Data Engineer Associate

### The exam scope:
- Databricks Lakehouse Platform – 24%
- ELT With Spark SQL and Python – 29%
- Incremental Data Processing – 22%
- Production Pipelines – 16%
- Data Governance – 9%

### Databricks Lakehouse Platform
- Managing Delta Tables : create table, insert into, select from, update and delete, drop table queries can be used for delta lake operations. Default mode of operation in DBR 8+ 
- Merge - Used for Upserts (Updates + Inserts) Syntax- << Merge INTO Destination Table using source table using conditions When matched When not matched >> 
	MERGE statements must have at least one field to match on, and each 
	WHEN MATCHED or WHEN NOT MATCHED clause can have any number of additional conditional statements
