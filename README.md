Kafka is useless since we use a capped collection. Simply start a change stream against "system.profile". We don't really care about resume tokens.
Ingest change stream, put it in internal Mongo Database (with TTL index).

When starting the program, if "system.profile" does not exists, create it and set max size (capped collection).

When starting the program, turn on profiler: db.setProfilingLevel(1, { slowms: 100 }) // Profiler will capture queries taking more than 100ms
When stopping the program, turn off profiler: db.setProfilingLevel(0)

https://www.mongodb.com/docs/manual/tutorial/manage-the-database-profiler/

UI need to allow sorting by multiple criteria. e.g. avg duration + count (i.e. Give me what really matters in term of duration)