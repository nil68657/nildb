package query

import (
	"testing"

	"github.com/nil68657/nildb/internal/doc"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func canon(v bson.RawValue) string { return string(doc.FormatEJSON(oneField("v", v))) }

func TestExpressions(t *testing.T) {
	const date = `{"$date": "2021-06-15T10:00:00Z"}`
	cases := []struct {
		expr, doc string
		want      string // canonical {"v": ...}, "missing", or the error text
	}{
		{`"$a"`, `{"a": 5}`, `{"v":{"$numberInt":"5"}}`},
		{`"$a.b"`, `{"a": [{"b": 1}, {"b": 2}, {"c": 3}]}`, `{"v":[{"$numberInt":"1"},{"$numberInt":"2"}]}`},
		{`"$a.b"`, `{"a": [[{"b": 1}], {"b": 2}, 7]}`, `{"v":[[{"$numberInt":"1"}],{"$numberInt":"2"}]}`},
		{`"$a.0"`, `{"a": [5, 6]}`, `{"v":[]}`},
		{`"$missing"`, `{}`, "missing"},
		{`"$$ROOT"`, `{"a": 1}`, `{"v":{"a":{"$numberInt":"1"}}}`},
		{`"$$CURRENT.a"`, `{"a": 1}`, `{"v":{"$numberInt":"1"}}`},
		{`"$$REMOVE"`, `{"a": 1}`, "missing"},
		{`{"$literal": "$a"}`, `{}`, `{"v":"$a"}`},
		{`{"$add": [1, 2]}`, `{}`, `{"v":{"$numberInt":"3"}}`},
		{`{"$add": [2147483647, 1]}`, `{}`, `{"v":{"$numberLong":"2147483648"}}`},
		{`{"$add": [{"$numberLong": "9223372036854775807"}, 1]}`, `{}`, canon(f64(9223372036854775807))},
		{`{"$add": [1, 2.5]}`, `{}`, `{"v":{"$numberDouble":"3.5"}}`},
		{`{"$add": [` + date + `, 1000]}`, `{}`, `{"v":{"$date":{"$numberLong":"1623751201000"}}}`},
		{`{"$add": [1, null]}`, `{}`, `{"v":null}`},
		{`{"$add": [1, "$missing"]}`, `{}`, `{"v":null}`},
		{`{"$add": [1, "x"]}`, `{}`, "ERR $add only supports numeric or date types, not string"},
		{`{"$subtract": [5, 7]}`, `{}`, `{"v":{"$numberInt":"-2"}}`},
		{`{"$subtract": ["$b", "$a"]}`, `{"a": {"$date": "2021-06-15T10:00:00Z"}, "b": {"$date": "2021-06-15T10:00:02Z"}}`, `{"v":{"$numberLong":"2000"}}`},
		{`{"$subtract": [1]}`, `{}`, "ERR Expression $subtract takes exactly 2 arguments. 1 were passed in."},
		{`{"$multiply": [3, 4]}`, `{}`, `{"v":{"$numberInt":"12"}}`},
		{`{"$multiply": [65536, 65536]}`, `{}`, `{"v":{"$numberLong":"4294967296"}}`},
		{`{"$multiply": [2, 0.25]}`, `{}`, `{"v":{"$numberDouble":"0.5"}}`},
		{`{"$divide": [7, 2]}`, `{}`, `{"v":{"$numberDouble":"3.5"}}`},
		{`{"$divide": [1, 0]}`, `{}`, "ERR can't $divide by zero"},
		{`{"$divide": ["a", 1]}`, `{}`, "ERR $divide only supports numeric types, not string and int"},
		{`{"$mod": [7, 3]}`, `{}`, `{"v":{"$numberInt":"1"}}`},
		{`{"$mod": [7.5, 2]}`, `{}`, `{"v":{"$numberDouble":"1.5"}}`},
		{`{"$mod": [1, 0]}`, `{}`, "ERR can't $mod by zero"},
		{`{"$eq": ["$a", 5]}`, `{"a": 5.0}`, `{"v":true}`},
		{`{"$eq": ["$missing", null]}`, `{}`, `{"v":false}`},
		{`{"$lt": ["$missing", null]}`, `{}`, `{"v":true}`},
		{`{"$gt": ["abc", 5]}`, `{}`, `{"v":true}`},
		{`{"$ne": [1, 1.0]}`, `{}`, `{"v":false}`},
		{`{"$gte": [2, 2]}`, `{}`, `{"v":true}`},
		{`{"$lte": [3, 2]}`, `{}`, `{"v":false}`},
		{`{"$and": [1, "x", []]}`, `{}`, `{"v":true}`},
		{`{"$and": [1, 0]}`, `{}`, `{"v":false}`},
		{`{"$or": [null, false, 0]}`, `{}`, `{"v":false}`},
		{`{"$not": [0]}`, `{}`, `{"v":true}`},
		{`{"$cond": [true, "yes", "no"]}`, `{}`, `{"v":"yes"}`},
		{`{"$cond": {"if": true, "then": 1, "else": {"$divide": [1, 0]}}}`, `{}`, `{"v":{"$numberInt":"1"}}`},
		{`{"$cond": {"if": false, "then": 1, "else": {"$divide": [1, 0]}}}`, `{}`, "ERR can't $divide by zero"},
		{`{"$cond": {"if": true, "then": 1}}`, `{}`, "ERR Missing 'else' parameter to $cond"},
		{`{"$ifNull": ["$missing", "dflt"]}`, `{}`, `{"v":"dflt"}`},
		{`{"$ifNull": [null, null, 3]}`, `{}`, `{"v":{"$numberInt":"3"}}`},
		{`{"$ifNull": [0, 3]}`, `{}`, `{"v":{"$numberInt":"0"}}`},
		{`{"$concat": ["a", "b", "$s"]}`, `{"s": "c"}`, `{"v":"abc"}`},
		{`{"$concat": ["a", null]}`, `{}`, `{"v":null}`},
		{`{"$concat": ["a", 1]}`, `{}`, "ERR $concat only supports strings, not int"},
		{`{"$toLower": "ABc"}`, `{}`, `{"v":"abc"}`},
		{`{"$toUpper": "$missing"}`, `{}`, `{"v":""}`},
		{`{"$toUpper": 12}`, `{}`, `{"v":"12"}`},
		{`{"$toUpper": "straße"}`, `{}`, `{"v":"STRAßE"}`},
		{`{"$size": [[1, 2, 3]]}`, `{}`, `{"v":{"$numberInt":"3"}}`},
		{`{"$size": "$missing"}`, `{}`, "ERR The argument to $size must be an array. Type of argument was: missing"},
		{`{"$year": ` + date + `}`, `{}`, `{"v":{"$numberInt":"2021"}}`},
		{`{"$month": {"date": {"$date": "2021-06-30T23:00:00Z"}, "timezone": "+02:00"}}`, `{}`, `{"v":{"$numberInt":"7"}}`},
		{`{"$month": {"date": {"$date": "2021-06-30T23:00:00Z"}, "timezone": "Europe/London"}}`, `{}`, `{"v":{"$numberInt":"7"}}`},
		{`{"$dayOfMonth": ` + date + `}`, `{}`, `{"v":{"$numberInt":"15"}}`},
		{`{"$dayOfMonth": "$_id"}`, `{"_id": {"$oid": "60c87e800000000000000000"}}`, `{"v":{"$numberInt":"15"}}`},
		{`{"$year": "$missing"}`, `{}`, `{"v":null}`},
		{`{"$year": "x"}`, `{}`, "ERR can't convert from BSON type string to Date"},
		{`{"$month": {"date": ` + date + `, "timezone": "Mars/Base"}}`, `{}`, `ERR unrecognized time zone identifier: "Mars/Base"`},
		{`{"$sqrt": 4}`, `{}`, "ERR unsupported expression operator '$sqrt' in v1"},
		{`{"a": "$x", "b": {"$add": [1, 1]}, "c": "$missing"}`, `{"x": 1}`, `{"v":{"a":{"$numberInt":"1"},"b":{"$numberInt":"2"}}}`},
		{`["$x", "$missing"]`, `{"x": 1}`, `{"v":[{"$numberInt":"1"},null]}`},
		{`"$$NOPE"`, `{}`, "ERR Use of undefined variable: NOPE"},
		{`{"$add": [1], "$multiply": [2]}`, `{}`, "ERR an expression specification must contain exactly one field, the name of the expression. Found 2 fields"},
		{`{"$add": [{"$numberDecimal": "1.5"}, 1]}`, `{}`, "ERR Decimal128 is not supported in indexes or arithmetic in v1"},
	}
	for _, tc := range cases {
		raw := ej(t, `{"e": `+tc.expr+`}`)
		got := func() string {
			ex, err := CompileExpr(raw.Lookup("e"))
			if err != nil {
				return err.Error()
			}
			v, err := Eval(ex, ej(t, tc.doc))
			if err != nil {
				return err.Error()
			}
			if v.Type == 0 {
				return "missing"
			}
			return canon(v)
		}()
		if got != tc.want {
			t.Errorf("%s on %s = %s, want %s", tc.expr, tc.doc, got, tc.want)
		}
	}
}

func TestTruthy(t *testing.T) {
	for _, v := range []bson.RawValue{{}, nullV, undefinedV, boolV(false), i32(0), i64(0), f64(0)} {
		if truthy(v) {
			t.Errorf("truthy(%v) = true", v)
		}
	}
	for _, v := range []bson.RawValue{strV(""), arrV(nil), docV(emptyDocBytes), i32(-1), f64(0.5), boolV(true)} {
		if !truthy(v) {
			t.Errorf("truthy(%v) = false", v)
		}
	}
}
