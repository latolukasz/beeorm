package where

type prevOperator string

const (
	prevOperatorNone prevOperator = ""
	prevOperatorAnd  prevOperator = "AND"
	prevOperatorOr   prevOperator = "OR"
)

type operator string

const (
	nullOperator               operator = ""
	operatorEqual              operator = "="
	operatorNotEqual           operator = "!="
	operatorGreaterThan        operator = ">"
	operatorLessThan           operator = "<"
	operatorGreaterThanOrEqual operator = ">="
	operatorLessThanOrEqual    operator = "<="
	operatorIsNull             operator = "IS NULL"
	operatorIsNotNull          operator = "IS NOT NULL"
	operatorLike               operator = "LIKE"
	operatorNotLike            operator = "NOT LIKE"
	operatorIn                 operator = "IN"
	operatorNotIn              operator = "NOT IN"
	operatorBetween            operator = "BETWEEN"
	operatorNotBetween         operator = "NOT BETWEEN"
	operatorAnd                operator = "AND"
	operatorOr                 operator = "OR"
	operatorNot                operator = "NOT"
	operatorAddition           operator = "+"
	operatorSubtraction        operator = "-"
	operatorMultiplication     operator = "*"
	operatorDivision           operator = "/"
	operatorModulo             operator = "%"
	operatorIncrement          operator = "++"
	operatorBitwiseAnd         operator = "&"
	operatorBitwiseOr          operator = "|"
	operatorBitwiseXor         operator = "^"
	operatorBitwiseNot         operator = "~"
	operatorLeftShift          operator = "<<"
	operatorRightShift         operator = ">>"
	operatorAssign             operator = "="
	operatorAddAssign          operator = "+="
	operatorSubtractAssign     operator = "-="
	operatorMultiplyAssign     operator = "*="
	operatorDivideAssign       operator = "/="
	operatorModuloAssign       operator = "%="
	operatorBitwiseAndAssign   operator = "&="
	operatorBitwiseOrAssign    operator = "|="
	operatorBitwiseXorAssign   operator = "^="
	operatorLeftShiftAssign    operator = "<<="
	operatorRightShiftAssign   operator = ">>="
	operatorRegexp             operator = "REGEXP"
	operatorNotRegexp          operator = "NOT REGEXP"
	operatorDiv                operator = "DIV"
)

var allOperators = []operator{
	operatorEqual,
	operatorNotEqual,
	operatorGreaterThan,
	operatorLessThan,
	operatorGreaterThanOrEqual,
	operatorLessThanOrEqual,
	operatorIsNull,
	operatorIsNotNull,
	operatorLike,
	operatorNotLike,
	operatorIn,
	operatorNotIn,
	operatorBetween,
	operatorNotBetween,
	operatorAnd,
	operatorOr,
	operatorNot,
	operatorAddition,
	operatorSubtraction,
	operatorMultiplication,
	operatorDivision,
	operatorModulo,
	operatorIncrement,
	operatorBitwiseAnd,
	operatorBitwiseOr,
	operatorBitwiseXor,
	operatorBitwiseNot,
	operatorLeftShift,
	operatorRightShift,
	operatorAssign,
	operatorAddAssign,
	operatorSubtractAssign,
	operatorMultiplyAssign,
	operatorDivideAssign,
	operatorModuloAssign,
	operatorBitwiseAndAssign,
	operatorBitwiseOrAssign,
	operatorBitwiseXorAssign,
	operatorLeftShiftAssign,
	operatorRightShiftAssign,
	operatorRegexp,
	operatorNotRegexp,
	operatorDiv,
}

func isOp(op string) bool {
	for _, v := range allOperators {
		if op == string(v) {
			return true
		}
	}
	return false
}
