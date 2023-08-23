# Mongo Profiler

So far, only a log collector, but pretty nifty when it comes to finding what and who is causing load on the system.

## Run

You need a local MongoDB instance where we'll save the query logs and run queries against them.

1. `podman run -p 27017:27017 docker.io/library/mongo`
1. `go run profiler.go -listened="<MONGO_CONNECTION_STRING>" -v`

In Mongo 7.0, we have the $median and $percentile operators
- https://www.mongodb.com/docs/upcoming/reference/operator/aggregation/median/#mongodb-group-grp.-median
- https://www.mongodb.com/docs/upcoming/reference/operator/aggregation/percentile/#mongodb-group-grp.-percentile

Queries to get som interesting stats out of the data:

What's are the slowest calls:
```
db.getCollection("slowops").aggregate([
  { $sort: { durationMS: 1 } },
  {
    $group: {
      _id: { queryHash: "$queryHash", collection: "$collection", user: "$user" },
      cnt: { $sum: 1 },
      durations: { $push: "$durationMS" },
      sumDuration: { $sum: '$durationMS' },
      avgDuration: { $avg: "$durationMS" },
      minDuration: { $min: "$durationMS" },
      maxDuration: { $max: "$durationMS" },
    },
  },
  {
    $project: {
      _id: 1,
      cnt: 1,
      avgDuration: 1,
      sumDuration: 1,
      minDuration: 1,
      maxDuration: 1,
      p50: { $arrayElemAt: ["$durations", { $floor: { $multiply: [{ $size: "$durations" }, 0.5] } }] },
      p85: { $arrayElemAt: ["$durations", { $floor: { $multiply: [{ $size: "$durations" }, 0.85] } }] },
      p90: { $arrayElemAt: ["$durations", { $floor: { $multiply: [{ $size: "$durations" }, 0.9] } }] },
      p99: { $arrayElemAt: ["$durations", { $floor: { $multiply: [{ $size: "$durations" }, 0.99] } }] },
    },
  },
  { $sort: { sumDuration: -1 } },
  { $match: { cnt: { $gt: 10 } } },
]);
```

What user is making the most queries:
```
db.getCollection("slowops").aggregate([
  { $group: { _id: "$user", cnt: { $sum: 1 } } },
  { $sort: { cnt: -1 } },
])
```

Each entry has a `queryHash` field that you can use to query `slowops.examples` and see what the exact query looks like.
