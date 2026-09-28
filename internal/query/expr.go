package query

import (
	"encoding/binary"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nil68657/nildb/internal/doc"
	"github.com/nil68657/nildb/internal/keyenc"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Aggregation expressions, the surface scope-v1.md fixes for v1: field
// paths, literals, $$ROOT, $$CURRENT and $$REMOVE, $literal, arithmetic
// ($add $subtract $multiply $divide $mod), comparisons ($eq $ne $gt $gte
// $lt $lte), $and $or $not, $cond, $ifNull, $concat, $toLower, $toUpper,
// $size, $year, $month, $dayOfMonth. Any other operator fails at compile
// time with "unsupported expression operator '<op>' in v1".
//
// Semantics follow MongoDB's: null, undefined and a missing field make
// arithmetic, $concat and the date operators return null; numbers widen
// int32 -> int64 -> double, an int32 result that overflows becomes an
// int64 and an int64 overflow a double; comparisons use BSON order with a
// missing field below null (unlike the query language, where the two are
// equal); Decimal128 is refused in arithmetic.

// Expr is one compiled expression. eval returns the zero RawValue for a
// missing result.
type Expr interface {
	eval(root bson.Raw) (bson.RawValue, error)
}

var undefinedV = bson.RawValue{Type: bson.TypeUndefined}

// errDecimal is keyenc's Decimal128 refusal, the v1 rule for arithmetic
// and accumulators.
var errDecimal = &doc.Error{Code: doc.CodeBadValue, Msg: keyenc.ErrUnsupported.Error()}

func exprErr(code int, format string, a ...any) error {
	return &doc.Error{Code: code, Msg: fmt.Sprintf(format, a...)}
}

func unsupportedOp(op string) error {
	return exprErr(168, "unsupported expression operator '%s' in v1", op)
}

// CompileExpr compiles one aggregation expression.
func CompileExpr(v bson.RawValue) (Expr, error) { return compileExpr(v) }

// Eval evaluates e against the document root.
func Eval(e Expr, root bson.Raw) (bson.RawValue, error) { return e.eval(root) }

func compileExpr(v bson.RawValue) (Expr, error) {
	switch v.Type {
	case bson.TypeString:
		s, _ := strOf(v)
		switch {
		case strings.HasPrefix(s, "$$"):
			return compileVar(s[2:])
		case strings.HasPrefix(s, "$"):
			parts, err := fieldPath(s[1:])
			if err != nil {
				return nil, err
			}
			return pathExpr{parts}, nil
		}
		return constExpr{v}, nil
	case bson.TypeEmbeddedDocument:
		return compileObject(v.Value)
	case bson.TypeArray:
		var a arrayExpr
		for _, el := range arrayValues(v.Value) {
			e, err := compileExpr(el)
			if err != nil {
				return nil, err
			}
			a.exprs = append(a.exprs, e)
		}
		return a, nil
	}
	return constExpr{v}, nil
}

func fieldPath(p string) ([]string, error) {
	if p == "" {
		return nil, exprErr(16872, "'$' by itself is not a valid FieldPath")
	}
	parts := strings.Split(p, ".")
	for _, c := range parts {
		if c == "" {
			return nil, exprErr(15998, "FieldPath field names may not be empty strings.")
		}
		if c[0] == '$' {
			return nil, exprErr(16410, "FieldPath field names may not start with '$'.")
		}
	}
	return parts, nil
}

func compileVar(s string) (Expr, error) {
	name, rest, _ := strings.Cut(s, ".")
	switch name {
	case "ROOT", "CURRENT":
		if rest == "" {
			return pathExpr{}, nil
		}
		parts, err := fieldPath(rest)
		if err != nil {
			return nil, err
		}
		return pathExpr{parts}, nil
	case "REMOVE":
		return removeExpr{}, nil
	}
	return nil, exprErr(17276, "Use of undefined variable: %s", name)
}

func compileObject(d []byte) (Expr, error) {
	it := iterDoc(d)
	first, ok := it.next()
	if !ok {
		if it.bad {
			return nil, exprErr(doc.CodeBadValue, "malformed BSON document")
		}
		return constExpr{docV(d)}, nil
	}
	if len(first.name) > 0 && first.name[0] == '$' {
		n := 1
		for _, more := it.next(); more; _, more = it.next() {
			n++
		}
		if n > 1 {
			return nil, exprErr(15983, "an expression specification must contain exactly one field, the name of the expression. Found %d fields", n)
		}
		return compileOperator(string(first.name), first.value())
	}
	var obj objectExpr
	it = iterDoc(d)
	for e, ok := it.next(); ok; e, ok = it.next() {
		name := string(e.name)
		switch {
		case strings.HasPrefix(name, "$"):
			return nil, exprErr(16410, "FieldPath field names may not start with '$'.")
		case strings.Contains(name, "."):
			return nil, exprErr(16412, "FieldPath field names may not contain '.'.")
		}
		ex, err := compileExpr(e.value())
		if err != nil {
			return nil, err
		}
		obj.names = append(obj.names, name)
		obj.exprs = append(obj.exprs, ex)
	}
	return obj, nil
}

// opArgs returns an operator's arguments: the elements of an array, or the
// value itself as the only argument.
func opArgs(v bson.RawValue) []bson.RawValue {
	if v.Type == bson.TypeArray {
		return arrayValues(v.Value)
	}
	return []bson.RawValue{v}
}

func compileArgs(v bson.RawValue) ([]Expr, error) {
	vals := opArgs(v)
	out := make([]Expr, len(vals))
	for i, a := range vals {
		e, err := compileExpr(a)
		if err != nil {
			return nil, err
		}
		out[i] = e
	}
	return out, nil
}

func arity(op string, args []Expr, n int) error {
	if len(args) != n {
		return exprErr(16020, "Expression %s takes exactly %d arguments. %d were passed in.", op, n, len(args))
	}
	return nil
}

func compileOperator(op string, arg bson.RawValue) (Expr, error) {
	switch op {
	case "$literal":
		return constExpr{arg}, nil
	case "$cond":
		return compileCond(arg)
	case "$year", "$month", "$dayOfMonth":
		return compileDatePart(op, arg)
	}
	args, err := compileArgs(arg)
	if err != nil {
		return nil, err
	}
	fixed := func(n int, fn func([]bson.RawValue) (bson.RawValue, error)) (Expr, error) {
		if err := arity(op, args, n); err != nil {
			return nil, err
		}
		return funcExpr{args, fn}, nil
	}
	switch op {
	case "$add":
		return funcExpr{args, evalAdd}, nil
	case "$multiply":
		return funcExpr{args, evalMultiply}, nil
	case "$subtract":
		return fixed(2, evalSubtract)
	case "$divide":
		return fixed(2, evalDivide)
	case "$mod":
		return fixed(2, evalMod)
	case "$eq", "$ne", "$gt", "$gte", "$lt", "$lte":
		return fixed(2, cmpFunc(op))
	case "$and":
		return andExpr(args), nil
	case "$or":
		return orExpr(args), nil
	case "$not":
		return fixed(1, func(v []bson.RawValue) (bson.RawValue, error) { return boolV(!truthy(v[0])), nil })
	case "$ifNull":
		if len(args) < 2 {
			return nil, exprErr(1257300, "$ifNull needs at least two arguments, had: %d", len(args))
		}
		return ifNullExpr(args), nil
	case "$concat":
		return funcExpr{args, evalConcat}, nil
	case "$toLower":
		return fixed(1, func(v []bson.RawValue) (bson.RawValue, error) { return caseMap(v[0], false) })
	case "$toUpper":
		return fixed(1, func(v []bson.RawValue) (bson.RawValue, error) { return caseMap(v[0], true) })
	case "$size":
		return fixed(1, evalSize)
	}
	return nil, unsupportedOp(op)
}

type constExpr struct{ v bson.RawValue }

func (e constExpr) eval(bson.Raw) (bson.RawValue, error) { return e.v, nil }

// pathExpr is "$a.b" (or $$ROOT.a.b); no parts is $$ROOT itself.
type pathExpr struct{ parts []string }

func (e pathExpr) eval(root bson.Raw) (bson.RawValue, error) {
	if len(e.parts) == 0 {
		return docV(root), nil
	}
	return aggPath(root, e.parts), nil
}

type removeExpr struct{}

func (removeExpr) eval(bson.Raw) (bson.RawValue, error) { return bson.RawValue{}, nil }

// objectExpr is an expression object {a: <expr>, ...}; fields whose value
// is missing are left out.
type objectExpr struct {
	names []string
	exprs []Expr
}

func (e objectExpr) eval(root bson.Raw) (bson.RawValue, error) {
	var w builder
	w.begin()
	for i, ex := range e.exprs {
		v, err := ex.eval(root)
		if err != nil {
			return bson.RawValue{}, err
		}
		if v.Type != 0 {
			w.elem(e.names[i], v)
		}
	}
	return docV(w.end()), nil
}

// arrayExpr is an array of expressions; a missing element becomes null.
type arrayExpr struct{ exprs []Expr }

func (e arrayExpr) eval(root bson.Raw) (bson.RawValue, error) {
	vals := make([]bson.RawValue, len(e.exprs))
	for i, ex := range e.exprs {
		v, err := ex.eval(root)
		if err != nil {
			return bson.RawValue{}, err
		}
		if v.Type == 0 {
			v = nullV
		}
		vals[i] = v
	}
	return arrV(vals), nil
}

type funcExpr struct {
	args []Expr
	fn   func([]bson.RawValue) (bson.RawValue, error)
}

func (e funcExpr) eval(root bson.Raw) (bson.RawValue, error) {
	vals := make([]bson.RawValue, len(e.args))
	for i, a := range e.args {
		v, err := a.eval(root)
		if err != nil {
			return bson.RawValue{}, err
		}
		vals[i] = v
	}
	return e.fn(vals)
}

type andExpr []Expr

func (e andExpr) eval(root bson.Raw) (bson.RawValue, error) {
	for _, a := range e {
		v, err := a.eval(root)
		if err != nil {
			return bson.RawValue{}, err
		}
		if !truthy(v) {
			return boolV(false), nil
		}
	}
	return boolV(true), nil
}

type orExpr []Expr

func (e orExpr) eval(root bson.Raw) (bson.RawValue, error) {
	for _, a := range e {
		v, err := a.eval(root)
		if err != nil {
			return bson.RawValue{}, err
		}
		if truthy(v) {
			return boolV(true), nil
		}
	}
	return boolV(false), nil
}

// ifNullExpr returns its first argument that is not null, undefined or
// missing, else its last argument.
type ifNullExpr []Expr

func (e ifNullExpr) eval(root bson.Raw) (bson.RawValue, error) {
	for _, a := range e[:len(e)-1] {
		v, err := a.eval(root)
		if err != nil {
			return bson.RawValue{}, err
		}
		if !nullArg(v) {
			return v, nil
		}
	}
	return e[len(e)-1].eval(root)
}

type condExpr struct{ ifE, thenE, elseE Expr }

func (e condExpr) eval(root bson.Raw) (bson.RawValue, error) {
	v, err := e.ifE.eval(root)
	if err != nil {
		return bson.RawValue{}, err
	}
	if truthy(v) {
		return e.thenE.eval(root)
	}
	return e.elseE.eval(root)
}

// compileCond accepts [if, then, else] and {if, then, else}.
func compileCond(arg bson.RawValue) (Expr, error) {
	if arg.Type == bson.TypeEmbeddedDocument {
		if first, ok := lookupFirst(arg.Value); ok && !strings.HasPrefix(first, "$") {
			var c condExpr
			it := iterDoc(arg.Value)
			for e, ok := it.next(); ok; e, ok = it.next() {
				ex, err := compileExpr(e.value())
				if err != nil {
					return nil, err
				}
				switch string(e.name) {
				case "if":
					c.ifE = ex
				case "then":
					c.thenE = ex
				case "else":
					c.elseE = ex
				default:
					return nil, exprErr(17083, "Unrecognized parameter to $cond: %s", e.name)
				}
			}
			switch {
			case c.ifE == nil:
				return nil, exprErr(17080, "Missing 'if' parameter to $cond")
			case c.thenE == nil:
				return nil, exprErr(17081, "Missing 'then' parameter to $cond")
			case c.elseE == nil:
				return nil, exprErr(17082, "Missing 'else' parameter to $cond")
			}
			return c, nil
		}
	}
	args, err := compileArgs(arg)
	if err != nil {
		return nil, err
	}
	if err := arity("$cond", args, 3); err != nil {
		return nil, err
	}
	return condExpr{args[0], args[1], args[2]}, nil
}

func lookupFirst(d []byte) (string, bool) {
	it := iterDoc(d)
	e, ok := it.next()
	return string(e.name), ok
}

// nullArg reports null, undefined or a missing value, which make most
// operators return null.
func nullArg(v bson.RawValue) bool {
	return v.Type == 0 || v.Type == bson.TypeNull || v.Type == bson.TypeUndefined
}

// truthy is MongoDB's coerceToBool: false for false, null, undefined, a
// missing value and numeric zero; true for everything else, including ""
// and empty arrays.
func truthy(v bson.RawValue) bool {
	switch v.Type {
	case 0, bson.TypeNull, bson.TypeUndefined:
		return false
	case bson.TypeBoolean:
		return len(v.Value) == 1 && v.Value[0] != 0
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble:
		n, _ := numOf(v)
		return n.float() != 0
	case bson.TypeDecimal128:
		if d, ok := v.Decimal128OK(); ok {
			bi, _, err := d.BigInt()
			return err != nil || bi.Sign() != 0
		}
	}
	return true
}

// exprCompare orders two values for the comparison operators and $min and
// $max: BSON order, with a missing value equal to undefined and below null.
func exprCompare(a, b bson.RawValue) int {
	if a.Type == 0 {
		a = undefinedV
	}
	if b.Type == 0 {
		b = undefinedV
	}
	return doc.Compare(a, b)
}

func cmpFunc(op string) func([]bson.RawValue) (bson.RawValue, error) {
	return func(v []bson.RawValue) (bson.RawValue, error) {
		c := exprCompare(v[0], v[1])
		var r bool
		switch op {
		case "$eq":
			r = c == 0
		case "$ne":
			r = c != 0
		case "$gt":
			r = c > 0
		case "$gte":
			r = c >= 0
		case "$lt":
			r = c < 0
		default:
			r = c <= 0
		}
		return boolV(r), nil
	}
}

func dateMS(v bson.RawValue) int64 { return int64(binary.LittleEndian.Uint64(v.Value)) }

func evalAdd(vals []bson.RawValue) (bson.RawValue, error) {
	kind := bson.TypeInt32
	var isum int64
	var fsum float64
	overflow, anyDouble, hasDate := false, false, false
	var date int64
	for _, v := range vals {
		switch {
		case nullArg(v):
			return nullV, nil
		case v.Type == bson.TypeDateTime:
			if hasDate {
				return bson.RawValue{}, exprErr(16612, "only one date allowed in an $add expression")
			}
			hasDate, date = true, dateMS(v)
			continue
		case v.Type == bson.TypeDecimal128:
			return bson.RawValue{}, errDecimal
		}
		n, ok := numOf(v)
		if !ok {
			return bson.RawValue{}, exprErr(16554, "$add only supports numeric or date types, not %s", typeName(v.Type))
		}
		kind = widest(kind, n.kind)
		fsum += n.float()
		if n.isInt() && !overflow {
			isum, overflow = addInt64(isum, n.i)
		}
		if !n.isInt() {
			anyDouble = true
		}
	}
	switch {
	case hasDate && (anyDouble || overflow):
		return dateV(date + int64(math.Round(fsum))), nil
	case hasDate:
		return dateV(date + isum), nil
	case anyDouble || overflow:
		return f64(fsum), nil
	}
	return fitInt(kind, isum).value(), nil
}

func evalMultiply(vals []bson.RawValue) (bson.RawValue, error) {
	kind := bson.TypeInt32
	iprod, fprod := int64(1), 1.0
	overflow, anyDouble := false, false
	for _, v := range vals {
		switch {
		case nullArg(v):
			return nullV, nil
		case v.Type == bson.TypeDecimal128:
			return bson.RawValue{}, errDecimal
		}
		n, ok := numOf(v)
		if !ok {
			return bson.RawValue{}, exprErr(16555, "$multiply only supports numeric types, not %s", typeName(v.Type))
		}
		kind = widest(kind, n.kind)
		fprod *= n.float()
		if n.isInt() && !overflow {
			iprod, overflow = mulInt64(iprod, n.i)
		}
		if !n.isInt() {
			anyDouble = true
		}
	}
	if anyDouble || overflow {
		return f64(fprod), nil
	}
	return fitInt(kind, iprod).value(), nil
}

func evalSubtract(v []bson.RawValue) (bson.RawValue, error) {
	a, b := v[0], v[1]
	switch {
	case nullArg(a) || nullArg(b):
		return nullV, nil
	case a.Type == bson.TypeDecimal128 || b.Type == bson.TypeDecimal128:
		return bson.RawValue{}, errDecimal
	case a.Type == bson.TypeDateTime && b.Type == bson.TypeDateTime:
		return i64(dateMS(a) - dateMS(b)), nil
	case a.Type == bson.TypeDateTime:
		n, ok := numOf(b)
		if !ok {
			break
		}
		if n.isInt() {
			return dateV(dateMS(a) - n.i), nil
		}
		return dateV(dateMS(a) - int64(math.Round(n.f))), nil
	}
	na, okA := numOf(a)
	nb, okB := numOf(b)
	if !okA || !okB {
		return bson.RawValue{}, exprErr(16556, "can't $subtract %s from %s", typeName(b.Type), typeName(a.Type))
	}
	kind := widest(na.kind, nb.kind)
	if kind != bson.TypeDouble {
		d := na.i - nb.i
		if (na.i >= 0) == (nb.i >= 0) || (d >= 0) == (na.i >= 0) {
			return fitInt(kind, d).value(), nil
		}
	}
	return f64(na.float() - nb.float()), nil
}

func evalDivide(v []bson.RawValue) (bson.RawValue, error) {
	a, b := v[0], v[1]
	switch {
	case nullArg(a) || nullArg(b):
		return nullV, nil
	case a.Type == bson.TypeDecimal128 || b.Type == bson.TypeDecimal128:
		return bson.RawValue{}, errDecimal
	}
	na, okA := numOf(a)
	nb, okB := numOf(b)
	if !okA || !okB {
		return bson.RawValue{}, exprErr(16609, "$divide only supports numeric types, not %s and %s", typeName(a.Type), typeName(b.Type))
	}
	if nb.float() == 0 {
		return bson.RawValue{}, exprErr(16608, "can't $divide by zero")
	}
	return f64(na.float() / nb.float()), nil
}

func evalMod(v []bson.RawValue) (bson.RawValue, error) {
	a, b := v[0], v[1]
	switch {
	case nullArg(a) || nullArg(b):
		return nullV, nil
	case a.Type == bson.TypeDecimal128 || b.Type == bson.TypeDecimal128:
		return bson.RawValue{}, errDecimal
	}
	na, okA := numOf(a)
	nb, okB := numOf(b)
	if !okA || !okB {
		return bson.RawValue{}, exprErr(16611, "$mod only supports numeric types, not %s and %s", typeName(a.Type), typeName(b.Type))
	}
	if nb.float() == 0 {
		return bson.RawValue{}, exprErr(16610, "can't $mod by zero")
	}
	kind := widest(na.kind, nb.kind)
	if kind == bson.TypeDouble {
		return f64(math.Mod(na.float(), nb.float())), nil
	}
	return fitInt(kind, na.i%nb.i).value(), nil
}

func evalConcat(vals []bson.RawValue) (bson.RawValue, error) {
	var b strings.Builder
	for _, v := range vals {
		if nullArg(v) {
			return nullV, nil
		}
		s, ok := strOf(v)
		if !ok {
			return bson.RawValue{}, exprErr(16702, "$concat only supports strings, not %s", typeName(v.Type))
		}
		b.WriteString(s)
	}
	return strV(b.String()), nil
}

// caseMap is $toLower and $toUpper: ASCII letters only, as MongoDB
// documents; null or missing gives "", and numbers and dates are coerced
// to strings first.
func caseMap(v bson.RawValue, upper bool) (bson.RawValue, error) {
	s, err := coerceString(v)
	if err != nil {
		return bson.RawValue{}, err
	}
	b := []byte(s)
	for i, c := range b {
		switch {
		case upper && 'a' <= c && c <= 'z':
			b[i] = c - 'a' + 'A'
		case !upper && 'A' <= c && c <= 'Z':
			b[i] = c - 'A' + 'a'
		}
	}
	return strV(string(b)), nil
}

func coerceString(v bson.RawValue) (string, error) {
	switch v.Type {
	case 0, bson.TypeNull, bson.TypeUndefined:
		return "", nil
	case bson.TypeString, bson.TypeSymbol:
		s, _ := strOf(v)
		return s, nil
	case bson.TypeInt32, bson.TypeInt64:
		n, _ := numOf(v)
		return strconv.FormatInt(n.i, 10), nil
	case bson.TypeDouble:
		n, _ := numOf(v)
		return strconv.FormatFloat(n.f, 'g', -1, 64), nil
	case bson.TypeDateTime:
		return time.UnixMilli(dateMS(v)).UTC().Format("2006-01-02T15:04:05.000Z"), nil
	}
	return "", exprErr(16007, "can't convert from BSON type %s to String", typeName(v.Type))
}

func evalSize(v []bson.RawValue) (bson.RawValue, error) {
	if v[0].Type != bson.TypeArray {
		return bson.RawValue{}, exprErr(17124, "The argument to $size must be an array. Type of argument was: %s", typeName(v[0].Type))
	}
	n := 0
	it := iterDoc(v[0].Value)
	for _, ok := it.next(); ok; _, ok = it.next() {
		n++
	}
	return i32(int32(n)), nil
}

// datePartExpr is $year, $month or $dayOfMonth of a date, a timestamp or an
// ObjectId's creation time, in UTC or the given timezone.
type datePartExpr struct {
	op   string
	date Expr
	tz   Expr // nil for UTC
}

func compileDatePart(op string, arg bson.RawValue) (Expr, error) {
	e := datePartExpr{op: op}
	if arg.Type == bson.TypeEmbeddedDocument {
		if first, ok := lookupFirst(arg.Value); ok && !strings.HasPrefix(first, "$") {
			it := iterDoc(arg.Value)
			for f, ok := it.next(); ok; f, ok = it.next() {
				ex, err := compileExpr(f.value())
				if err != nil {
					return nil, err
				}
				switch string(f.name) {
				case "date":
					e.date = ex
				case "timezone":
					e.tz = ex
				default:
					return nil, exprErr(40535, "unrecognized option to %s: \"%s\"", op, f.name)
				}
			}
			if e.date == nil {
				return nil, exprErr(40539, "missing 'date' argument to %s", op)
			}
			return e, nil
		}
	}
	args, err := compileArgs(arg)
	if err != nil {
		return nil, err
	}
	if err := arity(op, args, 1); err != nil {
		return nil, err
	}
	e.date = args[0]
	return e, nil
}

func (e datePartExpr) eval(root bson.Raw) (bson.RawValue, error) {
	v, err := e.date.eval(root)
	if err != nil || nullArg(v) {
		return nullV, err
	}
	var ms int64
	switch v.Type {
	case bson.TypeDateTime:
		ms = dateMS(v)
	case bson.TypeTimestamp:
		ms = int64(binary.LittleEndian.Uint64(v.Value)>>32) * 1000
	case bson.TypeObjectID:
		ms = int64(binary.BigEndian.Uint32(v.Value)) * 1000
	default:
		return bson.RawValue{}, exprErr(16006, "can't convert from BSON type %s to Date", typeName(v.Type))
	}
	loc := time.UTC
	if e.tz != nil {
		tv, err := e.tz.eval(root)
		if err != nil || nullArg(tv) {
			return nullV, err
		}
		s, ok := strOf(tv)
		if !ok {
			return bson.RawValue{}, exprErr(40517, "timezone must evaluate to a string, found %s", typeName(tv.Type))
		}
		if loc, err = timezone(s); err != nil {
			return bson.RawValue{}, err
		}
	}
	t := time.UnixMilli(ms).In(loc)
	switch e.op {
	case "$year":
		return i32(int32(t.Year())), nil
	case "$month":
		return i32(int32(t.Month())), nil
	}
	return i32(int32(t.Day())), nil
}

var (
	tzOffset = regexp.MustCompile(`^([+-])(\d{2}):?(\d{2})?$`)
	tzMu     sync.Mutex
	tzCache  = map[string]*time.Location{}
)

// timezone resolves an Olson name or a UTC offset (+HH, +HHMM, +HH:MM).
func timezone(s string) (*time.Location, error) {
	tzMu.Lock()
	loc, ok := tzCache[s]
	tzMu.Unlock()
	if ok {
		return loc, nil
	}
	if m := tzOffset.FindStringSubmatch(s); m != nil {
		h, _ := strconv.Atoi(m[2])
		mins, _ := strconv.Atoi(m[3])
		secs := (h*60 + mins) * 60
		if m[1] == "-" {
			secs = -secs
		}
		loc = time.FixedZone(s, secs)
	} else {
		var err error
		if loc, err = time.LoadLocation(s); err != nil || s == "" || s == "Local" {
			return nil, exprErr(40485, "unrecognized time zone identifier: \"%s\"", s)
		}
	}
	tzMu.Lock()
	tzCache[s] = loc
	tzMu.Unlock()
	return loc, nil
}

// aggPath evaluates a field path with aggregation semantics: through an
// array, the rest of the path is applied to each element and the results
// form an array (nested arrays stay nested); numeric components name
// fields, not array positions.
func aggPath(d []byte, parts []string) bson.RawValue {
	v, ok := lookup(d, parts[0])
	if !ok {
		return bson.RawValue{}
	}
	return aggPathRest(v, parts[1:])
}

func aggPathRest(v bson.RawValue, rest []string) bson.RawValue {
	if len(rest) == 0 {
		return v
	}
	switch v.Type {
	case bson.TypeEmbeddedDocument:
		return aggPath(v.Value, rest)
	case bson.TypeArray:
		var out []bson.RawValue
		for _, el := range arrayValues(v.Value) {
			var r bson.RawValue
			switch el.Type {
			case bson.TypeEmbeddedDocument:
				r = aggPath(el.Value, rest)
			case bson.TypeArray:
				r = aggPathRest(el, rest)
			}
			if r.Type != 0 {
				out = append(out, r)
			}
		}
		return arrV(out)
	}
	return bson.RawValue{}
}

// typeName is MongoDB's name for a BSON type in error messages.
func typeName(t bson.Type) string {
	switch t {
	case 0:
		return "missing"
	case bson.TypeDouble:
		return "double"
	case bson.TypeString:
		return "string"
	case bson.TypeEmbeddedDocument:
		return "object"
	case bson.TypeArray:
		return "array"
	case bson.TypeBinary:
		return "binData"
	case bson.TypeUndefined:
		return "undefined"
	case bson.TypeObjectID:
		return "objectId"
	case bson.TypeBoolean:
		return "bool"
	case bson.TypeDateTime:
		return "date"
	case bson.TypeNull:
		return "null"
	case bson.TypeRegex:
		return "regex"
	case bson.TypeDBPointer:
		return "dbPointer"
	case bson.TypeJavaScript:
		return "javascript"
	case bson.TypeSymbol:
		return "symbol"
	case bson.TypeCodeWithScope:
		return "javascriptWithScope"
	case bson.TypeInt32:
		return "int"
	case bson.TypeTimestamp:
		return "timestamp"
	case bson.TypeInt64:
		return "long"
	case bson.TypeDecimal128:
		return "decimal"
	case bson.TypeMinKey:
		return "minKey"
	case bson.TypeMaxKey:
		return "maxKey"
	}
	return "unknown"
}
