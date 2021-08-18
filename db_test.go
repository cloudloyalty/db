package db

import (
	"testing"

	"github.com/shopspring/decimal"

	"github.com/stretchr/testify/assert"
)

type testStruct struct {
	StrValue string `json:"str_value,omitempty"`
	IntValue int    `json:"int_value"`
}

func TestQprintf(t *testing.T) {
	var strPtr *string
	var decimalPtr *decimal.Decimal
	var notInitializedSlice []testStruct
	var nullPointerToSlice *[]testStruct
	var nullPointerToArray *[2]testStruct
	var nullPointerToStruct *testStruct

	var cases = []struct {
		SQL            string
		params         Params
		expectedResult string
	}{
		// empty source
		{
			"",
			Params{},
			"",
		},
		// strings
		{
			":a, :b",
			Params{"a": "value_a", "b": "value_b"},
			"'value_a', 'value_b'",
		},
		// strings with null word
		{
			":a, :b, :c",
			Params{"a": "null", "b": "NULL", "c": "nil"},
			"'null', 'NULL', 'nil'",
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
			Params{"comma_list": CommaListParam{1, 2, 3, 4, 5, nil, 6, "as"}},
			"WHERE field IN (1, 2, 3, 4, 5, NULL, 6, 'as')",
		},
		// slice of scalars converts to json
		{
			":a, :b",
			Params{"a": []string{"AAA", "BBB"}, "b": []int{0, 2, 4}},
			"'[\"AAA\",\"BBB\"]', '[0,2,4]'",
		},
		// slice of struct converts to json
		{
			":a",
			Params{"a": []testStruct{{StrValue: "", IntValue: 42}}},
			"'[{\"int_value\":42}]'",
		},
		// empty slice
		{
			":a",
			Params{"a": []testStruct{}},
			"'[]'",
		},
		// not initialized slice
		{
			":a",
			Params{"a": notInitializedSlice},
			"NULL",
		},
		// null pointer to slice
		{
			":a",
			Params{"a": nullPointerToSlice},
			"NULL",
		},
		// null pointer to array
		{
			":a",
			Params{"a": nullPointerToArray},
			"NULL",
		},
		// pointer to empty slice
		{
			":a",
			Params{"a": &[]testStruct{}},
			"'[]'",
		},
		// pointer to slice of struct
		{
			":a",
			Params{"a": &[]testStruct{{StrValue: "24", IntValue: 42}}},
			"'[{\"str_value\":\"24\",\"int_value\":42}]'",
		},
		// struct
		{
			":a",
			Params{"a": testStruct{}},
			"'{\"int_value\":0}'",
		},
		// pointer to struct
		{
			":a",
			Params{"a": &testStruct{}},
			"'{\"int_value\":0}'",
		},
		// null pointer to struct
		{
			":a",
			Params{"a": nullPointerToStruct},
			"NULL",
		},
		// some edge cases
		{
			":",
			Params{},
			":",
		},
		{
			"::",
			Params{},
			"::",
		},
	}

	for _, c := range cases {
		result, err := qprintf(c.SQL, c.params)
		assert.NoError(t, err)
		assert.Equal(t, c.expectedResult, result)
	}
}
