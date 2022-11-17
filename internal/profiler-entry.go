package internal

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
)

type ProfilerEntry struct {
	Timestamp      time.Time `bson:"ts,omitempty"`
	OP             string    `bson:"op,omitempty"`
	Collection     string    `bson:"ns,omitempty"`
	User           string    `bson:"user,omitempty"`
	ResponseLength int       `bson:"responseLength,omitempty"`
	DurationMS     int       `bson:"millis,omitempty"`
	CursorID       int64     `bson:"cursorid,omitempty"`
	KeysExamined   int       `bson:"keysExamined,omitempty"`
	DocExamined    int       `bson:"docsExamined,omitempty"`
	HasSortStage   bool      `bson:"hasSortStage,omitempty"`
	NReturned      int       `bson:"nreturned,omitempty"`
	NDeleted       int       `bson:"ndeleted,omitempty"`
	NInserted      int       `bson:"ninserted,omitempty"`
	NModified      int       `bson:"nmodified,omitempty"`
	QueryHash      string    `bson:"queryHash,omitempty"`
	PlanHash       string    `bson:"planCacheKey,omitempty"`
	PlanSummary    string    `bson:"planSummary,omitempty"`
	Host           string
	Document       bson.Raw
}

func NewProfilerEntry(host string, data bson.Raw) (entry *ProfilerEntry, err error) {
	r, err := bson.NewDecoder(bsonrw.NewBSONDocumentReader(data))
	if err != nil {
		return nil, err
	}

	if err := r.Decode(&entry); err != nil {
		return nil, err
	}

	entry.Host = host
	entry.Document = data

	return entry, nil
}

func (entry *ProfilerEntry) ToSlowOpsRecord() *SlowOpsRecord {
	return &SlowOpsRecord{
		Host:           entry.Host,
		Timestamp:      entry.Timestamp,
		OP:             entry.OP,
		Collection:     entry.Collection,
		User:           entry.User,
		ResponseLength: entry.ResponseLength,
		DurationMS:     entry.DurationMS,
		CursorID:       entry.CursorID,
		KeysExamined:   entry.KeysExamined,
		DocExamined:    entry.DocExamined,
		HasSortStage:   entry.HasSortStage,
		NReturned:      entry.NReturned,
		NDeleted:       entry.NDeleted,
		NInserted:      entry.NInserted,
		NModified:      entry.NModified,
		QueryHash:      entry.queryHash(),
		PlanHash:       entry.PlanHash,
		PlanSummary:    entry.PlanSummary,
	}
}

func (entry *ProfilerEntry) ToSlowOpsExampleRecord() *SlowOpsExampleRecord {
	return &SlowOpsExampleRecord{
		QueryHash:   entry.queryHash(),
		PlanHash:    entry.PlanHash,
		PlanSummary: entry.PlanSummary,
		Document:    entry.Document,
	}
}

func (entry *ProfilerEntry) queryHash() string {
	if entry.QueryHash != "" {
		return entry.QueryHash // e.g. FFF0C0D3
	}

	// TODO: Hashing
	// strings.ToUpper(strconv.FormatInt(int64(adler32.Checksum([]byte("test"))), 16)) (at least look like the original: 45D01C1)

	// Could also use Murmur and convert it to hex (What MongoDB does internally):
	// https://github.com/scylladb/scylla-go-driver/tree/main/transport/murmur

	// TODO: Get all match + sort keys and use that to generate query hash

	// TODO: generate a hash manually

	// In case the query is empty, there is no queryHash or planCacheKey. Example distinct key on all collection
	// Java does label + db + col + op + fields + sort + projection

	// Mongo does: SimpleStringDataComparator::kInstance.hash (https://github.com/mongodb/mongo/blob/459f574b8a4afd9e2e843c625f2ee4b726da12f3/src/mongo/db/query/canonical_query_encoder.cpp#L1146)

	// https://www.mongodb.com/docs/v4.2/reference/glossary/#term-query-shape

	// Empty query hash:
	//	- distinct
	//  - getMore -> = useless noise
	//  - aggregate -> most important (but only seems to be when match was empty? Would be logical given we don't have fields to generate hash)

	// TODO: collect all keys used in first $match + $sort +

	return ""
}
