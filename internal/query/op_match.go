package query

import "github.com/nil68657/nildb/internal/doc"

// matchOp is a $match that could not be fused into the source, because a
// stage it cannot move past comes before it.
type matchOp struct {
	x  *Exec
	in Operator
	m  *doc.Matcher
	n  int
}

func (o *matchOp) Next() (Row, bool, error) {
	for {
		r, ok, err := o.in.Next()
		if err != nil || !ok {
			return r, ok, err
		}
		if o.n++; o.n&1023 == 0 {
			if err := o.x.checkDeadline(); err != nil {
				return Row{}, false, err
			}
		}
		ok, err = o.m.Match(r.Doc)
		if err != nil {
			return Row{}, false, err
		}
		if ok {
			return r, true, nil
		}
	}
}

func (o *matchOp) Suspend() { o.in.Suspend() }
func (o *matchOp) Close()   { o.in.Close() }
