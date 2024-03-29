package constant

import "time"

const MONGO_CONNECT_TIMEOUT = 2 * time.Second
const MONGO_SOCKET_TIMEOUT = 2 * time.Second
const MONGO_COLLECTION_EXISTS_ERROR = 48
const MONGO_INDEX_EXISTS_ERROR = 85
const MONGO_DUPLICATE_DOCUMENT_ERROR = 11000
const MONGO_CAPPED_POSITION_LOST_ERROR = 136
