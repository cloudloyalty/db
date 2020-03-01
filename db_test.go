package db

import (
	"testing"

	"github.com/shopspring/decimal"

	"github.com/stretchr/testify/assert"
)

func TestQprintf(t *testing.T) {
	var strPtr *string
	var decimalPtr *decimal.Decimal
	var cases = []struct {
		SQL            string
		params         Params
		expectedResult string
	}{
		// strings
		{
			":a, :b",
			Params{"a": "value_a", "b": "value_b"},
			"'value_a', 'value_b'",
		},
		// nil, nil pointer
		{
			":a, :b",
			Params{"a": nil, "b": strPtr},
			"NULL, NULL",
		},
		// overlapping param names
		{
			":a, :a_b, :a b",
			Params{"a": "value_a", "a_b": "value_b"},
			"'value_a', 'value_b', 'value_a' b",
		},
		// decimal, pointer to decimal
		{
			":a, :b",
			Params{"a": decimal.NewFromFloat(0.0005), "b": decimalPtr},
			"0.0005, NULL",
		},
		// param name collides with a type specification
		{
			"'1'::int",
			Params{"int": 1},
			"'1'::int",
		},
		// comma list
		{
			"WHERE field IN (:comma_list)",
			Params{"comma_list": commaList{1, 2, 3, 4, 5, nil, 6, "as"}},
			"WHERE field IN (1, 2, 3, 4, 5, NULL, 6, 'as')",
		},
	}

	for _, c := range cases {
		result, err := qprintf(c.SQL, c.params)
		assert.NoError(t, err)
		assert.Equal(t, c.expectedResult, result)
	}
}
