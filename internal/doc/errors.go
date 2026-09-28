package doc

import (
	"errors"
	"fmt"

	"github.com/nil68657/nildb/internal/keyenc"
)

// MongoDB error codes that doc errors carry. Handlers put them in
// writeErrors entries; the numbers are MongoDB's ErrorCodes values.
const (
	CodeBadValue                   = 2
	CodeFailedToParse              = 9
	CodeTypeMismatch               = 14
	CodePathNotViable              = 28
	CodeConflictingUpdateOperators = 40
	CodeInvalidIDField             = 53
	CodeNotSingleValueField        = 54
	CodeEmptyFieldName             = 56
	CodeImmutableField             = 66
	CodeBSONObjectTooLarge         = 10334
	CodeDocumentTooLarge           = 17419
	CodeInvalidRegex               = 51091
	CodeInvalidRegexOptions        = 51108
)

// Error is a document-layer failure: a malformed filter, update, projection
// or document, or an update that MongoDB would refuse. Msg is MongoDB's
// errmsg text where MongoDB documents one; Error() prefixes "ERR " so a
// handler can reply resp.Err(err.Error()) directly.
type Error struct {
	Code int    // MongoDB error code for writeErrors entries
	Msg  string // text without the "ERR " prefix
}

func (e *Error) Error() string { return "ERR " + e.Msg }

func errf(code int, format string, a ...any) error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, a...)}
}

func badValue(format string, a ...any) error { return errf(CodeBadValue, format, a...) }

// errMalformed reports BSON bytes that do not parse.
var errMalformed = &Error{Code: CodeBadValue, Msg: "malformed BSON document"}

// errDecimal is keyenc's Decimal128 refusal as a doc error, for $inc, $mul,
// _id and anything else that must turn a Decimal128 into key bytes or do
// arithmetic on one.
var errDecimal = &Error{Code: CodeBadValue, Msg: keyenc.ErrUnsupported.Error()}

// ErrNoGeoHooks is returned by Compile for a filter that uses $geoWithin,
// $geoIntersects, $near or $nearSphere when no GeoHooks were supplied.
var ErrNoGeoHooks = &Error{Code: CodeBadValue, Msg: "geo operators need a geo index or hooks"}

// Code returns the MongoDB error code carried by err, or CodeBadValue for
// errors that did not come from this package.
func Code(err error) int {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return CodeBadValue
}
