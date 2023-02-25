package collector

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/guillotjulien/mongo-profiler/internal/utils/murmur3"
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
		Collection:  entry.Collection,
		PlanHash:    entry.PlanHash,
		PlanSummary: entry.PlanSummary,
		Document:    entry.Document,
	}
}

// Query Shape:
// > A combination of query predicate, sort, projection, and collation.
// > The query shape allows MongoDB to identify logically equivalent queries and analyze their performance.
//
// https://www.mongodb.com/docs/manual/reference/glossary/#std-term-query-shape
func (entry *ProfilerEntry) queryHash() string {
	if entry.QueryHash != "" {
		return entry.QueryHash // e.g. FFF0C0D3
	}

	// Need to be able to differentiate them
	if entry.OP == "insert" || entry.OP == "delete" || entry.OP == "update" {
		hash, _ := murmur3.Hash([]byte(fmt.Sprintf("%s-%s", entry.OP, entry.Collection)), 0)
		return strings.ToUpper(strconv.FormatInt(hash, 16))[:8]

		// Or something slightly resembling if murmur3 doesn't cut it
		// return strings.ToUpper(strconv.FormatInt(int64(adler32.Checksum([]byte(fmt.Sprintf("%s-%s", entry.OP, entry.Collection)))), 16))
	}

	// Could we parse the query by passing it to the Mongo driver so that we can get the query + sort + projection?
	// This way we could extract a query shape.

	return ""
}
