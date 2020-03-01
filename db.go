package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/kisielk/sqlstruct"
	"github.com/shopspring/decimal"
)

const DateTimeTzFormat = "2006-01-02 15:04:05.999999999-07"

type Params map[string]interface{}

type commaList []interface{}

var paramMatchRegexp = regexp.MustCompile("(^|[^:]):\\w+")

type Error struct {
	error
	Query  string
	Params Params
}

func wrapError(err error, sql string, params Params) error {
	if err == nil {
		return nil
	}
	return &Error{
		error:  err,
		Query:  sql,
		Params: params,
	}
}

func qprintf(sql string, params Params) (string, error) {
	var err error
	sql = paramMatchRegexp.ReplaceAllStringFunc(
		sql,
		func(matched string) string {
			// once an error occurred, skip other params
			if err != nil {
				return matched
			}
			param := matched
			var precedingChar string
			// any character preceding ':' that was captured by reg expr
			// should not be included in param name
			if matched[0] != ':' {
				param = param[1:]
				precedingChar = string(matched[0])
			}
			// skip : at beginning
			param = param[1:]
			v, ok := params[param]
			if !ok {
				err = fmt.Errorf("parameter %s is missing", param)
				return matched
			}
			var castedValue string
			castedValue, err = toDbValue(v)
			return precedingChar + castedValue
		},
	)
	if err != nil {
		return "", err
	}
	return sql, nil
}

func Exec(db Queryable, sql string, params Params) (sql.Result, error) {
	query, err := qprintf(sql, params)
	if err != nil {
		return nil, wrapError(err, sql, params)
	}
	res, err := db.Exec(query)
	if err != nil {
		return nil, wrapError(err, sql, params)
	}
	return res, nil
}

func Query(db Queryable, sql string, params Params) (*sql.Rows, error) {
	query, err := qprintf(sql, params)
	if err != nil {
		return nil, wrapError(err, sql, params)
	}
	rows, err := db.Query(query)
	if err != nil {
		return nil, wrapError(err, sql, params)
	}
	return rows, nil
}

func QueryRow(db Queryable, sql string, params Params) (*sql.Row, error) {
	query, err := qprintf(sql, params)
	if err != nil {
		return nil, wrapError(err, sql, params)
	}
	return db.QueryRow(query), nil
}

func QueryRowAndScan(db Queryable, q string, params Params, dest ...interface{}) error {
	row, err := QueryRow(db, q, params)
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

func QueryJSONRowIntoStruct(db Queryable, q string, params Params, target interface{}) error {
	row, err := QueryRow(db, q, params)
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

func ScanJSONRowsIntoStruct(rows *sql.Rows, target interface{}) error {
	var data []byte
	if err := rows.Scan(&data); err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

func QueryRowIntoStruct(db Queryable, q string, params Params, target interface{}) error {
	rows, err := Query(db, q, params)
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

func QueryRowsIntoSlice(db Queryable, q string, params Params, target interface{}) (interface{}, error) {
	rows, err := Query(db, q, params)
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
	case commaList:
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
	return quoteLiteral(string(encoded)), nil
}

// quoteLiteral properly escapes string to be safely
// passed as a value in SQL query
func quoteLiteral(s string) string {
	var p string
	if strings.Contains(s, `\`) {
		p = "E"
	}
	s = strings.Replace(s, `'`, `''`, -1)
	s = strings.Replace(s, `\`, `\\`, -1)
	return p + `'` + s + `'`
}
