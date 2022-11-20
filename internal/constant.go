package internal

import (
	"time"
)

const CONNECT_TIMEOUT = 2 * time.Second
const SOCKET_TIMEOUT = 2 * time.Second
const SYSTEM_PROFILE = "system.profile"
const SYSTEM_PROFILE_MAX_SIZE = 1024 * 1024 // 1MB
const MAX_RETRY = 3
const RETRY_AFTER = 10 * time.Second
const QUERY_SLOW_MS = 100 // Profiler's default value
const SLOWOPS_COLLECTION = "slowops"
const SLOWOPS_EXAMPLE_COLLECTION = "slowops.examples"
const SLOWOPS_EXPIRE_SECONDS = 7884000 // 3 months
