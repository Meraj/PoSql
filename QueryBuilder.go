package PoSql

import (
	"database/sql"
	_ "github.com/lib/pq"
	"math"
	"strconv"
	"strings"
)

type QueryBuilder struct {
	DB *sql.DB

	vars Variables
}
type Variables struct {
	table          string
	columns        []string
	whereStatement string
	orderBy        string
	limitOffset    string
	args           []interface{}
	currentNum     int
}

func (qb QueryBuilder) Table(table string) QueryBuilder {
	qb.vars.table = table
	qb.vars.currentNum = 1
	return qb
}

func (qb QueryBuilder) Select(columns ...string) QueryBuilder {
	qb.vars.columns = append(qb.vars.columns, columns...)
	return qb
}

func (qb QueryBuilder) Where(column string, value interface{}) QueryBuilder {
	if qb.vars.whereStatement == "" {
		qb.vars.whereStatement = " WHERE " + column + " = $" + strconv.Itoa(qb.vars.currentNum) + " "
	} else {
		qb.vars.whereStatement += " AND " + column + " = $" + strconv.Itoa(qb.vars.currentNum) + " "
	}
	qb.vars.args = append(qb.vars.args, value)
	qb.vars.currentNum = qb.vars.currentNum + 1
	return qb
}

func (qb QueryBuilder) WhereIsNull(column string) QueryBuilder {
	if qb.vars.whereStatement == "" {
		qb.vars.whereStatement = " WHERE " + column + " IS NULL "
	} else {
		qb.vars.whereStatement += " AND " + column + " IS NULL "
	}
	return qb
}

func (qb QueryBuilder) OrWhereIsNull(column string) QueryBuilder {
	qb.vars.whereStatement += " OR " + column + " IS NULL "
	return qb
}

func (qb QueryBuilder) WhereIsNotNull(column string) QueryBuilder {
	if qb.vars.whereStatement == "" {
		qb.vars.whereStatement = " WHERE " + column + " IS NOT NULL "
	} else {
		qb.vars.whereStatement += " AND " + column + " IS NULL "
	}
	return qb
}

func (qb QueryBuilder) OrWhereIsNotNull(column string) QueryBuilder {
	qb.vars.whereStatement += " OR " + column + " IS NOT NULL "
	return qb
}

func (qb QueryBuilder) OrWhere(column string, value interface{}) QueryBuilder {
	qb.vars.whereStatement += " OR " + column + " = $" + strconv.Itoa(qb.vars.currentNum) + " "
	qb.vars.args = append(qb.vars.args, value)
	qb.vars.currentNum = qb.vars.currentNum + 1
	return qb
}

func (qb QueryBuilder) WhereWithOperation(column string, operation string, value interface{}) QueryBuilder {
	if qb.vars.whereStatement == "" {
		qb.vars.whereStatement = " WHERE " + column + " " + operation + " = $" + strconv.Itoa(qb.vars.currentNum) + " "
	} else {
		qb.vars.whereStatement += " AND " + column + " " + operation + " = $" + strconv.Itoa(qb.vars.currentNum) + " "
	}
	qb.vars.args = append(qb.vars.args, value)
	qb.vars.currentNum = qb.vars.currentNum + 1

	return qb
}

func (qb QueryBuilder) OrWhereWithOperation(column string, operation string, value interface{}) QueryBuilder {
	qb.vars.whereStatement += " OR " + column + " " + operation + " = $" + strconv.Itoa(qb.vars.currentNum) + " "
	qb.vars.args = append(qb.vars.args, value)
	qb.vars.currentNum = qb.vars.currentNum + 1
	return qb
}

func (qb QueryBuilder) OrderBy(column string, orderType string) QueryBuilder {
	qb.vars.orderBy = "ORDER BY " + column + " " + orderType
	return qb
}

func (qb QueryBuilder) Limit(limitInt int, offsetInt int) QueryBuilder {
	qb.vars.limitOffset = " LIMIT " + strconv.Itoa(limitInt) + " OFFSET " + strconv.Itoa(offsetInt)
	return qb
}

func (qb QueryBuilder) buildQuery(SqlType int) string {
	var query = ""
	switch SqlType {
	case 0:
		qb.vars.currentNum = 1
		query = "INSERT INTO " + qb.vars.table + " ("
		query += strings.Join(qb.vars.columns, " ,")
		if last := len(query) - 1; last >= 0 && query[last] == ',' {
			query = query[:last]
		}
		query += ") VALUES ("
		for range qb.vars.columns {
			query += " $" + strconv.Itoa(qb.vars.currentNum) + " ,"
			qb.vars.currentNum = qb.vars.currentNum + 1
		}

		if last := len(query) - 1; last >= 0 && query[last] == ',' {
			query = query[:last]
		}
		query += ")"
		return query + "  RETURNING id"
	case 1:
		query = "SELECT "
		if qb.vars.columns == nil {
			query += " * "
		} else {
			query += strings.Join(qb.vars.columns, " ,")
			if last := len(query) - 1; last >= 0 && query[last] == ',' {
				query = query[:last]
			}
		}
		query += " FROM " + qb.vars.table + " "
	case 2:
		query = "UPDATE " + qb.vars.table + " SET "
		for _, column := range qb.vars.columns {
			query += column + " = $" + strconv.Itoa(qb.vars.currentNum) + " ,"
			qb.vars.currentNum = qb.vars.currentNum + 1
		}
		if last := len(query) - 1; last >= 0 && query[last] == ',' {
			query = query[:last]
		}
		break

	case 3:
		query = "DELETE FROM " + qb.vars.table + " "
	}
	if qb.vars.whereStatement != "" {
		query += " " + qb.vars.whereStatement
		qb.vars.whereStatement = ""
	}
	if qb.vars.orderBy != "" {
		query += " " + qb.vars.orderBy
		qb.vars.orderBy = ""
	}
	if qb.vars.limitOffset != "" {
		query += " " + qb.vars.limitOffset
		qb.vars.limitOffset = ""
	}
	qb.vars.columns = nil

	return query

}
func (qb QueryBuilder) Insert(theData map[string]interface{}) (int64, error) {
	qb.vars.args = nil
	qb.vars.columns = nil
	for key, element := range theData {
		qb.vars.columns = append(qb.vars.columns, key)
		qb.vars.args = append(qb.vars.args, element)
	}
	var id int64
	err := qb.DB.QueryRow(qb.buildQuery(0), qb.vars.args...).Scan(&id)
	qb.vars.args = nil
	if err != nil {
		id = 0
	}
	return id, err
}

func (qb QueryBuilder) First() *sql.Row {
	row := qb.DB.QueryRow(qb.buildQuery(1), qb.vars.args...)
	qb.vars.args = nil
	return row
}

func (qb QueryBuilder) Get() (*sql.Rows, error) {
	row, err := qb.DB.Query(qb.buildQuery(1), qb.vars.args...)
	qb.vars.args = nil
	return row, err
}

func (qb QueryBuilder) Update(theData map[string]interface{}) (sql.Result, error) {
	qb.vars.columns = nil
	for key, element := range theData {
		qb.vars.columns = append(qb.vars.columns, key)
		qb.vars.args = append(qb.vars.args, element)
	}
	res, err := qb.DB.Exec(qb.buildQuery(2), qb.vars.args...)
	qb.vars.args = nil
	return res, err
}

func (qb QueryBuilder) Delete() (int64, error) {
	res, err := qb.DB.Exec(qb.buildQuery(3), qb.vars.args...)
	count, err := res.RowsAffected()
	qb.vars.args = nil
	return count, err

}
func (qb QueryBuilder) Count() (int,error) {
	qb.vars.columns = []string{"COUNT(*) AS total"}
	var total int
	err := qb.First().Scan(&total)
	return total,err
}

func (qb QueryBuilder) Paginate(itemsPerPage int, currentPage int) PaginateModel {
	count,_ :=qb.Count()
	totalPages := int(math.Ceil(float64(count / itemsPerPage)))
	limitInt := (currentPage - 1) * itemsPerPage
	qb.Limit(itemsPerPage, limitInt)
	var paginateModel PaginateModel
	paginateModel.TotalPages = totalPages + 1
	paginateModel.CurrentPage = currentPage
	paginateModel.ResultsPerPage = itemsPerPage
	paginateModel.Rows, _ = qb.Get()
	return paginateModel
}
