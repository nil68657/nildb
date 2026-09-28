package query

// limitOp passes the first n rows and then closes its input, so the
// sources below release their iterators as soon as the limit is met.
type limitOp struct {
	in   Operator
	n    int64
	seen int64
}

func (o *limitOp) Next() (Row, bool, error) {
	if o.seen >= o.n {
		o.in.Close()
		return Row{}, false, nil
	}
	r, ok, err := o.in.Next()
	if err != nil || !ok {
		return r, ok, err
	}
	o.seen++
	return r, true, nil
}

func (o *limitOp) Suspend() { o.in.Suspend() }
func (o *limitOp) Close()   { o.in.Close() }

// skipOp drops the first n rows.
type skipOp struct {
	in      Operator
	n       int64
	skipped int64
}

func (o *skipOp) Next() (Row, bool, error) {
	for o.skipped < o.n {
		_, ok, err := o.in.Next()
		if err != nil || !ok {
			return Row{}, ok, err
		}
		o.skipped++
	}
	return o.in.Next()
}

func (o *skipOp) Suspend() { o.in.Suspend() }
func (o *skipOp) Close()   { o.in.Close() }
