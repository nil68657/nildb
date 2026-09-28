package query

import (
	"strings"

	"github.com/nil68657/nildb/internal/doc"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// countOp is the $count stage: it drains its input and emits one document
// {name: n}, or nothing when the input was empty, as MongoDB's rewrite of
// $count into $group {_id: null} does.
type countOp struct {
	x    *Exec
	in   Operator
	name string
	done bool
}

func (o *countOp) Next() (Row, bool, error) {
	if o.done {
		return Row{}, false, nil
	}
	o.done = true
	var n int64
	for {
		_, ok, err := o.in.Next()
		if err != nil {
			return Row{}, false, err
		}
		if !ok {
			break
		}
		if n++; n&1023 == 0 {
			if err := o.x.checkDeadline(); err != nil {
				return Row{}, false, err
			}
		}
	}
	o.in.Close()
	if n == 0 {
		return Row{}, false, nil
	}
	return Row{Doc: oneField(o.name, intValue(n))}, true, nil
}

func (o *countOp) Suspend() {
	if !o.done {
		o.in.Suspend()
	}
}

func (o *countOp) Close() { o.in.Close(); o.done = true }

// parseCountName validates the argument of $count with MongoDB's messages.
func parseCountName(v bson.RawValue) (string, error) {
	name, ok := strOf(v)
	switch {
	case !ok || v.Type != bson.TypeString || name == "":
		return "", &doc.Error{Code: 40156, Msg: "the count field must be a non-empty string"}
	case strings.HasPrefix(name, "$"):
		return "", &doc.Error{Code: 40158, Msg: "the count field cannot be a $-prefixed path"}
	case strings.Contains(name, "."):
		return "", &doc.Error{Code: 40160, Msg: "the count field cannot contain '.'"}
	}
	return name, nil
}
