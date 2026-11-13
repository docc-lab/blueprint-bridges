package snbounded

// Tomislav-RetCtx: snbounded is the Blueprint DeathStarBench Social Network workflow (package socialnetwork) rewritten
// to match upstream DeathStarBench (commit 6ecb097, 2024-06-27, socialNetwork/) operation for operation (user
// 2026-11-06: "Make the blueprint implementation MATCH UPSTREAM COMPLETELY"; timelines grow without limit exactly as
// upstream's do). Every function names the upstream lines it implements. What Blueprint itself replaces and this
// package therefore cannot match: Thrift RPC (gRPC here), OpenTracing/Jaeger spans (Blueprint's OpenTelemetry
// instrumentation here) and nginx + Lua as the HTTP entry (Wrk2APIService here). The user and social-graph services call
// each other as upstream's do. The package name predates the user's decision to drop the timeline cap; nothing in it is
// bounded.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"github.com/blueprint-uservices/blueprint/runtime/plugins/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

// From: https://gist.github.com/tsilvers/085c5f39430ced605d970094edf167ba
// Tomislav-RetCtx: upstream GetMachineId (UserService/UserHandler.h, the function after HashMacAddressPid) returns the
// last three hex digits, zero-padded, of a 16-bit hash of the MAC address, so an id is always 3 + 10 + 3 = 16 hex digits.
// The hash itself stays the Blueprint port's: upstream's HashMacAddressPid indexes the MAC string up to the length of
// MAC+pid, reading past its end, which has no defined result to reproduce.
func GetMachineID() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "000"
	}

	for _, i := range interfaces {
		if i.Flags&net.FlagUp != 0 && bytes.Compare(i.HardwareAddr, nil) != 0 {

			// Skip locally administered addresses
			if i.HardwareAddr[0]&2 == 2 {
				continue
			}

			var mac uint64
			for j, b := range i.HardwareAddr {
				if j >= 8 {
					break
				}
				mac <<= 8
				mac += uint64(b)
			}

			// Convert from uint64 to uint16
			mac_ui16 := uint16(mac)
			return lastHex(int64(mac_ui16), 3)
		}
	}

	return "000"
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// Converts a json-encoded string to a bson.D document to be used as arguments for one of the other NoSQLDatabase functions
// Tomislav-RetCtx: kept from the Blueprint port; snbounded builds its queries as bson.D directly, as upstream builds
// them with BSON_APPEND/BCON_NEW.
func parseNoSQLDBQuery(query string) (bson.D, error) {
	return handleFormats(query)
}

func lower(f interface{}) interface{} {
	switch f := f.(type) {
	case []interface{}:
		for i := range f {
			f[i] = lower(f[i])
		}
		return f
	case map[string]interface{}:
		lf := make(map[string]interface{}, len(f))
		for k, v := range f {
			if k == "$elemMatch" {
				lf[k] = lower(v)
			} else {
				lf[strings.ToLower(k)] = lower(v)
			}
		}
		return lf
	default:
		return f
	}
}

func handleFormats(jsonQuery string) (bdoc bson.D, err error) {

	if jsonQuery == "" {
		bdoc = bson.D{}
		return
	}

	var f interface{}
	err = json.Unmarshal([]byte(jsonQuery), &f)
	if err != nil {
		return
	}

	f = lower(f)

	lowerQuery, err := json.Marshal(f)
	if err != nil {
		return
	}
	err = bson.UnmarshalExtJSON(lowerQuery, true, &bdoc)
	return
}

// Tomislav-RetCtx: lastHex is upstream's formatting of id fields (UniqueIdHandler.h lines 81-101, UserHandler.h lines
// 255-274): the value in hex, keeping the last n digits (erase from the front) or zero-padding to n.
func lastHex(value int64, n int) string {
	text := strconv.FormatInt(value, 16)
	if len(text) > n {
		return text[len(text)-n:]
	}
	return strings.Repeat("0", n-len(text)) + text
}

// Tomislav-RetCtx: ensureIndex is upstream's start-up loop `while (!r) { r = CreateIndex(...); if (!r) sleep(1); }`
// (UserTimelineService.cpp lines 98-105; the same loop in SocialGraphService.cpp, PostStorageService.cpp,
// UserService.cpp, UrlShortenService.cpp) around CreateIndex (utils_mongodb.h, from line 49): an ascending, optionally
// unique, single-field index, retried once a second until it exists. A backend without CreateIndex is reported and
// skipped.
func ensureIndex(db backend.NoSQLDatabase, dbName, collectionName, field string, unique bool) error {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		supported := true
		err := func() error {
			collection, err := db.GetCollection(ctx, dbName, collectionName)
			if err != nil {
				return err
			}
			// Tomislav-RetCtx: CreateIndex is outside backend.NoSQLCollection; the MongoDB plugin's collection is reached
			// through its concrete type (an interface declared in this package could be matched by Blueprint's workflow
			// parser as a backend client's service interface).
			creator, ok := collection.(*mongodb.MongoCollection)
			if !ok {
				supported = false
				return nil
			}
			return creator.CreateIndex(ctx, field, unique)
		}()
		cancel()
		if !supported {
			log.Printf("snbounded: %s.%s backend cannot create indexes; %s stays unindexed", dbName, collectionName, field)
			return nil
		}
		if err == nil {
			log.Printf("snbounded: index %s.%s(%s) unique=%v ready", dbName, collectionName, field, unique)
			return nil
		}
		log.Printf("snbounded: failed to create mongodb index %s.%s(%s) (%v), try again", dbName, collectionName, field, err)
		time.Sleep(time.Second)
	}
}

// Tomislav-RetCtx: legacyJSON renders a BSON document exactly as libbson's bson_as_json (legacy extended JSON) does,
// which is what upstream caches for a post (PostStorageHandler.h lines 292, 531): "{ " + `"key" : value` pairs joined
// by ", " + " }" (an empty document is "{  }"), arrays "[ " ... " ]" ("[  ]" when empty), ObjectId as
// { "$oid" : "<hex>" }, integers as plain numbers, strings with '"', '\' and control characters escaped and every other
// byte (including '/' and non-ASCII UTF-8) as is. Checked against libbson 1.21.0's output for a post-shaped document
// (2026-11-06).
func legacyJSON(doc bson.Raw) ([]byte, error) {
	var buf bytes.Buffer
	if err := writeLegacyDocument(&buf, doc, false); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeLegacyDocument(buf *bytes.Buffer, doc bson.Raw, array bool) error {
	elements, err := doc.Elements()
	if err != nil {
		return err
	}
	open, close := "{ ", " }"
	if array {
		open, close = "[ ", " ]"
	}
	buf.WriteString(open)
	for i, element := range elements {
		if i > 0 {
			buf.WriteString(", ")
		}
		if !array {
			writeLegacyString(buf, element.Key())
			buf.WriteString(" : ")
		}
		if err := writeLegacyValue(buf, element.Value()); err != nil {
			return err
		}
	}
	buf.WriteString(close)
	return nil
}

func writeLegacyValue(buf *bytes.Buffer, value bson.RawValue) error {
	switch value.Type {
	case bsontype.EmbeddedDocument:
		return writeLegacyDocument(buf, value.Document(), false)
	case bsontype.Array:
		return writeLegacyDocument(buf, value.Array(), true)
	case bsontype.String:
		writeLegacyString(buf, value.StringValue())
	case bsontype.Int32:
		buf.WriteString(strconv.FormatInt(int64(value.Int32()), 10))
	case bsontype.Int64:
		buf.WriteString(strconv.FormatInt(value.Int64(), 10))
	case bsontype.Double:
		text := fmt.Sprintf("%.20g", value.Double()) // libbson legacy: "%.20g", ".0" appended to integral values
		if !strings.ContainsAny(text, ".eEn") {
			text += ".0"
		}
		buf.WriteString(text)
	case bsontype.Boolean:
		if value.Boolean() {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case bsontype.Null:
		buf.WriteString("null")
	case bsontype.ObjectID:
		buf.WriteString(`{ "$oid" : "` + value.ObjectID().Hex() + `" }`)
	default:
		return errors.New("legacyJSON: unsupported BSON type " + value.Type.String())
	}
	return nil
}

func writeLegacyString(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			buf.WriteString(`\"`)
		case '\\':
			buf.WriteString(`\\`)
		case '\b':
			buf.WriteString(`\b`)
		case '\f':
			buf.WriteString(`\f`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		default:
			if c < 0x20 {
				fmt.Fprintf(buf, `\u%04x`, c)
			} else {
				buf.WriteByte(c)
			}
		}
	}
	buf.WriteByte('"')
}
