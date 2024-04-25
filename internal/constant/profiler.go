package constant

const PROFILER_SYSTEM_PROFILE = "system.profile"
const PROFILER_SYSTEM_PROFILE_CAPPED_INCREMENT = 1024 * 1024 // 1MB
const PROFILER_SYSTEM_PROFILE_MAX_SIZE = 1024 * 1024 * 1024  // 1GB
const PROFILER_QUERY_SLOW_MS = 100                           // Profiler's default value // FIXME: Allow configuring that
const PROFILER_SLOWOPS_COLLECTION = "slowops"
const PROFILER_SLOWOPS_EXAMPLE_COLLECTION = "slowops.examples"
const PROFILER_SLOWOPS_EXPIRE_SECONDS = 7884000 // 3 months
