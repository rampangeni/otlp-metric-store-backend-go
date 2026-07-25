package main

import (
	"encoding/binary"
	"sort"
	"strconv"

	"github.com/cespare/xxhash/v2"
)

// MetricIdentity captures every field that together identifies a single metric
// time series, as opposed to one data point within that series. Two data points
// with an identical MetricIdentity belong to the same series and must therefore
// map to the same row in the otel_metric_metadata lookup table; two data points
// that differ in any of these fields (e.g. a resource attribute changed after a
// redeploy) are, by definition, different series and get their own metadata row.
//
// This is intentionally exactly the set of columns stored in otel_metric_metadata
// (see clickhouse_schema.go) minus the bookkeeping columns (MetricID itself,
// FirstSeenTimeUnix, CreatedAt).
type MetricIdentity struct {
	MetricType             string
	ServiceName            string
	ResourceAttributes     map[string]string
	ResourceSchemaUrl      string
	ScopeName              string
	ScopeVersion           string
	ScopeAttributes        map[string]string
	ScopeDroppedAttrCount  uint32
	ScopeSchemaUrl         string
	MetricName             string
	MetricDescription      string
	MetricUnit             string
	Attributes             map[string]string
	AggregationTemporality int32
	IsMonotonic            bool
}

// ID computes a stable 64-bit fingerprint of the identity. It is the value
// stored as MetricID in both the metadata lookup table and every datapoint
// table, and is what joins the two.
//
// The hash must be deterministic across process restarts and across the
// cluster (multiple backend replicas hashing the same series must agree), so:
//   - map iteration order is never relied upon: every map's keys are sorted first.
//   - every variable-length field is length-prefixed before hashing, so that
//     e.g. identity {Name: "ab", Unit: "c"} can never collide with
//     {Name: "a", Unit: "bc"}.
func (id MetricIdentity) ID() uint64 {
	h := xxhash.New()
	writeField(h, id.MetricType)
	writeField(h, id.ServiceName)
	writeSortedMap(h, id.ResourceAttributes)
	writeField(h, id.ResourceSchemaUrl)
	writeField(h, id.ScopeName)
	writeField(h, id.ScopeVersion)
	writeSortedMap(h, id.ScopeAttributes)
	writeField(h, strconv.FormatUint(uint64(id.ScopeDroppedAttrCount), 10))
	writeField(h, id.ScopeSchemaUrl)
	writeField(h, id.MetricName)
	writeField(h, id.MetricDescription)
	writeField(h, id.MetricUnit)
	writeSortedMap(h, id.Attributes)
	writeField(h, strconv.FormatInt(int64(id.AggregationTemporality), 10))
	writeField(h, strconv.FormatBool(id.IsMonotonic))
	return h.Sum64()
}

// writeField writes a length-prefixed string into the hasher.
func writeField(h *xxhash.Digest, s string) {
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(s)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write([]byte(s))
}

// writeSortedMap writes a map into the hasher in a deterministic (sorted by
// key) order, length-prefixing both the entry count and every key/value.
func writeSortedMap(h *xxhash.Digest, m map[string]string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(keys)))
	_, _ = h.Write(lenBuf[:])
	for _, k := range keys {
		writeField(h, k)
		writeField(h, m[k])
	}
}
