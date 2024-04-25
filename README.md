Kafka is useless since we use a capped collection. Simply start a change stream against "system.profile". We don't really care about resume tokens.
Ingest change stream, put it in internal Mongo Database (with TTL index).

When starting the program, if "system.profile" does not exists, create it and set max size (capped collection).

When starting the program, turn on profiler: db.setProfilingLevel(1, { slowms: 100 }) // Profiler will capture queries taking more than 100ms
When stopping the program, turn off profiler: db.setProfilingLevel(0)

https://www.mongodb.com/docs/manual/tutorial/manage-the-database-profiler/

UI need to allow sorting by multiple criteria. e.g. avg duration + count (i.e. Give me what really matters in term of duration)

Stats that could be interesting:
 - Top 5 queries (in terms of number / duration)
 - Top 5 collections in terms of number of queries (or number of documents scanned?)
 - A view per collection w/ following stats (could just be filters?):
    - Number of queries running without indexes
    - Number of queries slower than X
 - A mean to check long running queries across members of a replica set
 - A place where we can monitor live running queries (like Atlas does it?)

## Test

1. `podman run -p 27017:27017 docker.io/library/mongo`
1. ``


db.getCollection("slowops").aggregate([
  {
    $group: {
      _id: { queryHash: "$queryHash", collection: "$collection" },
      cnt: { $sum: 1 },
      avgDuration: { $avg: "$durationMS" },
      minDuration: { $min: "$durationMS" },
      maxDuration: { $max: "$durationMS" },
    },
  },
  { $sort: { avgDuration: -1 } },
  { $match: { cnt: { $gt: 10 } } },
]);
