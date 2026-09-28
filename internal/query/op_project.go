package query

import "github.com/nil68657/nildb/internal/doc"

// projectOp applies an inclusion or exclusion projection: DOC.FIND's
// PROJECT, and $project or $unset stages without computed fields, which
// follow the same rules in MongoDB.
type projectOp struct {
	in Operator
	p  *doc.Projection
}

func (o *projectOp) Next() (Row, bool, error) {
	r, ok, err := o.in.Next()
	if err != nil || !ok {
		return r, ok, err
	}
	out, err := o.p.Apply(r.Doc)
	if err != nil {
		return Row{}, false, err
	}
	r.Doc = out
	return r, true, nil
}

func (o *projectOp) Suspend() { o.in.Suspend() }
func (o *projectOp) Close()   { o.in.Close() }
