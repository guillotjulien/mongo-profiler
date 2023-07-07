# TODO

- [ ] Collector
  - [ ] [MEDIUM] Implement manual query shape detection
  - [ ] [MEDIUM] Recover from more errors
  - [ ] [MEDIUM] Allow configuration of constants (via CLI or conf file)
  - [ ] [LOW] Prevent duplicated records when recovering tailable cursor
  - [ ] [LOW] More granular logging
  - [ ] [LOW] Systemd service file (or profiler install command)
- [ ] Profiler UI
  - [ ] [HIGH] List of queries aggregated by query shape + collection
    - [ ] [HIGH] Sort / Filter
    - [ ] [LOW] Export (can use Mongo queries for that)
  - [ ] [MEDIUM] Top 5 queries by number / duration (2 charts)
  - [ ] [MEDIUM] Top 5 collections by number of queries / number of documents / keys scanned
  - [ ] [MEDIUM] Top 5 queries running without indexes
  - [ ] [LOW] A place to monitor live running queries like Atlas? 
- [ ] Documentation
  - [ ] [LOW] How to run