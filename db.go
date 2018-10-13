package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const DateTimeTzFormat = "2006-01-02 15:04:05.999999999-07"

type Params map[string]interface{}

func qprintf(sql string, params Params) (string, error) {
	var i int64
	var err error
	indexedParams := make([]interface{}, len(params))
	for k, v := range params {
		sql = strings.Replace(sql, ":"+k, "%["+strconv.FormatInt(i+1, 10)+"]s", -1)
		indexedParams[i], err = toDbValue(v)
		if err != nil {
			return "", err
		}
		i++
	}
	return fmt.Sprintf(sql, indexedParams...), nil
}

func Exec(db Queryable, sql string, params Params) (sql.Result, error) {
	query, err := qprintf(sql, params)
	if err != nil {
		return nil, err
	}
	return db.Exec(query)
}

func Query(db Queryable, sql string, params Params) (*sql.Rows, error) {
	query, err := qprintf(sql, params)
	if err != nil {
		return nil, err
	}
	return db.Query(query)
}

func QueryRow(db Queryable, sql string, params Params) (*sql.Row, error) {
	query, err := qprintf(sql, params)
	if err != nil {
		return nil, err
	}
	return db.QueryRow(query), nil
}

func QueryRowIntoStruct(db Queryable, sql string, params Params, target interface{}) error {
	row, err := QueryRow(db, sql, params)
	if err != nil {
		return err
	}
	var data []byte
	err = row.Scan(&data)
	if err != nil {
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
	if value, ok := value.(string); ok {
		return quoteLiteral(value), nil
	}
	if value, ok := value.(int); ok {
		return strconv.Itoa(value), nil
	}
	if value, ok := value.(float64); ok {
		return strconv.FormatFloat(value, 'g', -1, 64), nil
	}
	if value, ok := value.(bool); ok {
		return strconv.FormatBool(value), nil
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
