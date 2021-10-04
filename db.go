package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/kisielk/sqlstruct"
	"github.com/shopspring/decimal"
)

const DateTimeTzFormat = "2006-01-02 15:04:05.999999999-07"

type Params map[string]interface{}
type CommaListParam []interface{}

type Error struct {
	cause  error
	Query  string
	Params Params
}

func (e *Error) Error() string {
	if e.cause == nil {
		return "<cause is nil>"
	}
	v := reflect.ValueOf(e.cause)
	switch v.Kind() {
	case reflect.Ptr, reflect.UnsafePointer, reflect.Interface:
		if v.IsNil() {
			stackSlice := make([]byte, 512)
			s := runtime.Stack(stackSlice, false)
			stackStr := fmt.Sprintf("\n%s", stackSlice[0:s])
			return "<cause has nil value behind non-nil interface>" + stackStr
		}
	}
	return e.cause.Error()
}

func (e *Error) Cause() error {
	return e.cause
}

func wrapError(err error, sql string, params Params) error {
	if err == nil {
		return nil
	}
	return &Error{
		cause:  err,
		Query:  sql,
		Params: params,
	}
}

func qprintf(sql string, params Params) (string, error) {
	isNotWordChar := func(r rune) bool {
		return !((r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '_')
	}
	var result strings.Builder
	s := sql
	for {
		idx := strings.IndexRune(s, ':')
		// ':' not found or its at the end of the string
		if idx == -1 || idx == len(s)-1 {
			result.WriteString(s)
			break
		}
		// followed by a non-word char?
		if isNotWordChar(rune(s[idx+1])) {
			result.WriteString(s[:idx+2])
			s = s[idx+2:]
			continue
		}
		result.WriteString(s[:idx])
		s = s[idx+1:] // skip ':' char
		// find next \W character
		idxEnd := strings.IndexFunc(s, isNotWordChar)
		var param string
		if idxEnd == -1 {
			param = s
		} else {
			param = s[:idxEnd]
		}
		v, ok := params[param]
		if !ok {
			return "", fmt.Errorf("parameter %s is missing", param)
		}
		castedValue, err := toDbValue(v)
		if err != nil {
			return "", err
		}
		result.WriteString(castedValue)
		if idxEnd == -1 {
			break
		}
		s = s[idxEnd:]
	}
	return result.String(), nil
}

func Exec(ctx context.Context, db Queryable, sql string, params Params) (sql.Result, error) {
	query, err := qprintf(sql, params)
	if err != nil {
		return nil, wrapError(err, sql, params)
	}
	res, err := db.ExecContext(ctx, query)
	if err != nil {
		return nil, wrapError(err, sql, params)
	}
	return res, nil
}

func Query(ctx context.Context, db Queryable, sql string, params Params) (*sql.Rows, error) {
	query, err := qprintf(sql, params)
	if err != nil {
		return nil, wrapError(err, sql, params)
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, wrapError(err, sql, params)
	}
	return rows, nil
}

func QueryRow(ctx context.Context, db Queryable, sql string, params Params) (*sql.Row, error) {
	query, err := qprintf(sql, params)
	if err != nil {
		return nil, wrapError(err, sql, params)
	}
	return db.QueryRowContext(ctx, query), nil
}

func QueryRowAndScan(ctx context.Context, db Queryable, q string, params Params, dest ...interface{}) error {
	row, err := QueryRow(ctx, db, q, params)
	if err != nil {
		return err
	}
	if err := row.Scan(dest...); err != nil {
		if err == sql.ErrNoRows {
			return err
		}
		return wrapError(err, q, params)
	}
	return nil
}

func QueryJSONRowIntoStruct(ctx context.Context, db Queryable, q string, params Params, target interface{}) error {
	row, err := QueryRow(ctx, db, q, params)
	if err != nil {
		return err
	}
	var data []byte
	if err = row.Scan(&data); err != nil {
		if err == sql.ErrNoRows {
			return err
		}
		return wrapError(err, q, params)
	}
	if err = json.Unmarshal(data, target); err != nil {
		return wrapError(err, q, params)
	}
	return nil
}

func QueryRowIntoStruct(ctx context.Context, db Queryable, q string, params Params, target interface{}) error {
	rows, err := Query(ctx, db, q, params)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		return sql.ErrNoRows
	}
	if err = sqlstruct.Scan(target, rows); err != nil {
		return wrapError(err, q, params)
	}
	return nil
}

func QueryRowsIntoSlice(ctx context.Context, db Queryable, q string, params Params, target interface{}) (interface{}, error) {
	rows, err := Query(ctx, db, q, params)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	elemType := reflect.TypeOf(target)
	v := reflect.MakeSlice(reflect.SliceOf(elemType), 0, 0)
	for rows.Next() {
		elemPtr := reflect.New(elemType)
		if err := sqlstruct.Scan(elemPtr.Interface(), rows); err != nil {
			return nil, wrapError(err, q, params)
		}
		elem := reflect.Indirect(elemPtr)
		v = reflect.Append(v, elem)
	}
	return v.Interface(), nil
}

func ScanJSONRowsIntoStruct(rows *sql.Rows, target interface{}) error {
	var data []byte
	if err := rows.Scan(&data); err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

// toDbValue prepares value to be passed in SQL query
// with respect to its type and converts it to string
func toDbValue(value interface{}) (string, error) {
	if value == nil {
		return "NULL", nil
	}
	switch value := value.(type) {
	case *string:
		if value == nil {
			return "NULL", nil
		}
		return toDbValue(*value)
	case string:
		return quoteLiteral(value), nil
	case *int:
		if value == nil {
			return "NULL", nil
		}
		return toDbValue(*value)
	case int:
		return strconv.Itoa(value), nil
	case *float64:
		if value == nil {
			return "NULL", nil
		}
		return toDbValue(*value)
	case float64:
		return strconv.FormatFloat(value, 'g', -1, 64), nil
	case *bool:
		if value == nil {
			return "NULL", nil
		}
		return toDbValue(*value)
	case bool:
		return strconv.FormatBool(value), nil
	case *decimal.Decimal:
		if value == nil {
			return "NULL", nil
		}
		return toDbValue(*value)
	case decimal.Decimal:
		return value.String(), nil
	case *time.Time:
		if value == nil {
			return "NULL", nil
		}
		return toDbValue(*value)
	case time.Time:
		return quoteLiteral(value.Format(DateTimeTzFormat)), nil
	case CommaListParam:
		e := make([]string, len(value))
		for i := range value {
			var err error
			e[i], err = toDbValue(value[i])
			if err != nil {
				return "", err
			}
		}
		return strings.Join(e, ", "), nil
	}
	// the value is either slice or map, so insert it as JSON string
	// fixme: marshaller doesn't know how to encode map[interface{}]interface{}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	asString := string(encoded)
	if encoded == nil || asString == "null" {
		return "NULL", nil
	}
	return quoteLiteral(asString), nil
}

// quoteLiteral properly escapes string to be safely
// passed as a value in SQL query
func quoteLiteral(s string) string {
	var b strings.Builder
	b.Grow(len(s)*2 + 3)

	b.WriteRune('E')
	b.WriteRune('\'')

	hasSlash := false
	for _, c := range s {
		if c == '\\' {
			b.WriteString(`\\`)
			hasSlash = true
		} else if c == '\'' {
			b.WriteString(`''`)
		} else {
			b.WriteRune(c)
		}
	}

	b.WriteRune('\'')

	s = b.String()
	if !hasSlash {
		// remove unnecessary E at the beginning
		return s[1:]
	}
	return s
}
