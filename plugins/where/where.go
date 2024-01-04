package where

import (
	"fmt"
	"reflect"
	"strings"
)

type param struct {
	prevOperator prevOperator
	operator     operator
	col          string
	params       []any
}

type Query struct {
	query string

	parameters []param
}

func (w *Query) String() string {
	return strings.Trim(w.query, " ")
}

func (w *Query) GetParameters() []any {
	var params []any
	for _, p := range w.parameters {
		params = append(params, p.params...)
	}
	return params
}

func New() *Query {
	return &Query{}
}

func (w *Query) queryWithOperator(prev prevOperator, op operator, col string, params ...any) *Query {
	_op := prev
	if len(w.parameters) <= 0 {
		_op = prevOperatorNone
	}

	_cleanParams := make([]any, 0)
	for _, p := range params {
		switch v := p.(type) {
		case prevOperator:
			continue
		case operator:
			op = v
			continue
		case string:
			skipped := false
			for _, o := range allOperators {
				if o == operator(v) {
					op = operator(v)
					skipped = true
					break
				}
			}
			if skipped {
				continue
			}
		}
		_cleanParams = append(_cleanParams, p)
	}

	_params := make([]any, 0)

	for _, p := range _cleanParams {
		switch reflect.TypeOf(p).Kind() {
		case reflect.Slice, reflect.Array:
			val := reflect.ValueOf(p)
			length := val.Len()
			in := strings.Repeat(",?", length)
			in = strings.TrimLeft(in, ",")
			w.query += fmt.Sprintf(" %s `%s` IN (%v)", _op, col, in)
			_params = append(_params, p.([]any)...)

		default:
			switch op {
			case operatorIn:
				switch reflect.TypeOf(p).Kind() {
				case reflect.Slice, reflect.Array:
					val := reflect.ValueOf(p)
					length := val.Len()
					in := strings.Repeat(",?", length)
					in = strings.TrimLeft(in, ",")
					w.query += fmt.Sprintf(" %s `%s` IN (%v)", _op, col, in)
					_params = append(_params, p.([]any)...)
				case reflect.String:
					w.query += fmt.Sprintf(" %s `%s` IN (?)", _op, col)
				}

			default:
				w.query += fmt.Sprintf(" %s `%s` %s ?", _op, col, op)
			}

			_params = append(_params, p)
		}
	}

	w.parameters = append(w.parameters, param{
		prevOperator: prev,
		operator:     op,
		col:          col,
		params:       _params,
	})

	return w
}

func (w *Query) AndCustom(col string, params ...any) *Query {
	op := nullOperator
	for _, p := range params {
		switch p.(type) {
		case operator:
			op = p.(operator)
		case string:
			for _, o := range allOperators {
				if o == operator(p.(string)) {
					op = o
					break
				}
			}
		}
	}

	if op == nullOperator {
		panic("no operator was provided for AND clause")
	}

	return w.queryWithOperator(prevOperatorAnd, op, col, removeIfTwoOpsPresentAndKeepFirstOne(params...)...)
}

func (w *Query) OrCustom(col string, params ...any) *Query {
	op := nullOperator
	for _, p := range params {
		switch p.(type) {
		case operator:
			op = p.(operator)
		case string:
			for _, o := range allOperators {
				if o == operator(p.(string)) {
					op = o
					break
				}
			}
		}
	}

	if op == nullOperator {
		panic("no operator was provided for OR clause")
	}

	return w.queryWithOperator(prevOperatorOr, op, col, removeIfTwoOpsPresentAndKeepFirstOne(params...)...)
}

func removeIfTwoOpsPresentAndKeepFirstOne(ops ...any) []any {
	_ops := make([]any, 0)
	hasOps := false
	for _, op := range ops {
		switch v := op.(type) {
		case operator:
			if hasOps {
				continue
			}
			hasOps = true
		case string:
			if isOp(v) {
				if hasOps {
					continue
				}
				hasOps = true
			}
		}
		_ops = append(_ops, op)
	}
	return _ops
}

func (w *Query) Or(col string, params ...any) *Query {
	params = append(params, operatorEqual)
	return w.OrCustom(col, params...)
}

func (w *Query) And(col string, params ...any) *Query {
	params = append(params, operatorEqual)
	return w.AndCustom(col, params...)
}

func (w *Query) AndEqual(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorAnd, operatorEqual, col, params...)
}

func (w *Query) AndNotEqual(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorAnd, operatorNotEqual, col, params...)
}

func (w *Query) AndGreaterThan(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorAnd, operatorGreaterThan, col, params...)
}

func (w *Query) AndLessThan(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorAnd, operatorLessThan, col, params...)
}

func (w *Query) AndGreaterThanOrEqual(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorAnd, operatorGreaterThanOrEqual, col, params...)
}

func (w *Query) AndLessThanOrEqual(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorAnd, operatorLessThanOrEqual, col, params...)
}

func (w *Query) AndIsNull(col string) *Query {
	return w.queryWithOperator(prevOperatorAnd, operatorIsNull, col)
}

func (w *Query) AndIsNotNull(col string) *Query {
	return w.queryWithOperator(prevOperatorAnd, operatorIsNotNull, col)
}

func (w *Query) AndLike(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorAnd, operatorLike, col, params...)
}

func (w *Query) AndNotLike(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorAnd, operatorNotLike, col, params...)
}

func (w *Query) AndIn(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorAnd, operatorIn, col, params...)
}

func (w *Query) AndNotIn(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorAnd, operatorNotIn, col, params...)
}

func (w *Query) AndBetween(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorAnd, operatorBetween, col, params...)
}

func (w *Query) AndNotBetween(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorAnd, operatorNotBetween, col, params...)
}

func (w *Query) AndRegexp(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorAnd, operatorRegexp, col, params...)
}

func (w *Query) AndNotRegexp(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorAnd, operatorNotRegexp, col, params...)
}

func (w *Query) AndDiv(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorAnd, operatorDiv, col, params...)
}

func (w *Query) OrEqual(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorOr, operatorEqual, col, params...)
}

func (w *Query) OrNotEqual(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorOr, operatorNotEqual, col, params...)
}

func (w *Query) OrGreaterThan(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorOr, operatorGreaterThan, col, params...)
}

func (w *Query) OrLessThan(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorOr, operatorLessThan, col, params...)
}

func (w *Query) OrGreaterThanOrEqual(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorOr, operatorGreaterThanOrEqual, col, params...)
}

func (w *Query) OrLessThanOrEqual(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorOr, operatorLessThanOrEqual, col, params...)
}

func (w *Query) OrIsNull(col string) *Query {
	return w.queryWithOperator(prevOperatorOr, operatorIsNull, col)
}

func (w *Query) OrIsNotNull(col string) *Query {
	return w.queryWithOperator(prevOperatorOr, operatorIsNotNull, col)
}

func (w *Query) OrLike(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorOr, operatorLike, col, params...)
}

func (w *Query) OrNotLike(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorOr, operatorNotLike, col, params...)
}

func (w *Query) OrIn(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorOr, operatorIn, col, params...)
}

func (w *Query) OrNotIn(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorOr, operatorNotIn, col, params...)
}

func (w *Query) OrBetween(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorOr, operatorBetween, col, params...)
}

func (w *Query) OrNotBetween(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorOr, operatorNotBetween, col, params...)
}

func (w *Query) OrRegexp(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorOr, operatorRegexp, col, params...)
}

func (w *Query) OrNotRegexp(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorOr, operatorNotRegexp, col, params...)
}

func (w *Query) OrDiv(col string, params ...any) *Query {
	return w.queryWithOperator(prevOperatorOr, operatorDiv, col, params...)
}
