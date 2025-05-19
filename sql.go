package main

import (
	"fmt"
	"strconv"
	"strings"
)

type SQLStatement interface{}

type InsertStatement struct {
	Table  string
	Values map[string]string
}

type SelectStatement struct {
	Table   string
	Columns []string
	Where   map[string]string
	OrderBy string
	Limit   int
}

type UpdateStatement struct {
	Table string
	Set   map[string]string
	Where map[string]string
}

type DeleteStatement struct {
	Table string
	Where map[string]string
}

// Transaction statements

type BeginStatement struct{}
type CommitStatement struct{}
type RollbackStatement struct{}

// SQL parser enhancements: CREATE TABLE, CREATE INDEX, and more expressive WHERE

type CreateTableStatement struct {
	Table  string
	Schema []string
}

type CreateIndexStatement struct {
	Table   string
	Index   string
	Columns []string
}

// Very basic SQL parser for demo (supports simple INSERT, SELECT, UPDATE, DELETE)
func ParseSQL(sql string) (SQLStatement, error) {
	sql = strings.TrimSpace(sql)
	sqlU := strings.ToUpper(sql)
	if strings.HasPrefix(sqlU, "BEGIN") {
		return &BeginStatement{}, nil
	} else if strings.HasPrefix(sqlU, "COMMIT") {
		return &CommitStatement{}, nil
	} else if strings.HasPrefix(sqlU, "ROLLBACK") {
		return &RollbackStatement{}, nil
	} else if strings.HasPrefix(sqlU, "INSERT") {
		return parseInsert(sql)
	} else if strings.HasPrefix(sqlU, "SELECT") {
		return parseSelect(sql)
	} else if strings.HasPrefix(sqlU, "UPDATE") {
		return parseUpdate(sql)
	} else if strings.HasPrefix(sqlU, "DELETE") {
		return parseDelete(sql)
	} else if strings.HasPrefix(sqlU, "CREATE TABLE") {
		parts := strings.SplitN(sql, "(", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid CREATE TABLE syntax")
		}
		table := strings.Fields(parts[0])[2]
		schema := strings.Split(strings.TrimSuffix(parts[1], ")"), ",")
		for i := range schema {
			schema[i] = strings.ToLower(strings.TrimSpace(schema[i]))
		}
		return &CreateTableStatement{Table: table, Schema: schema}, nil
	} else if strings.HasPrefix(sqlU, "CREATE INDEX") {
		// Support standard SQL: CREATE INDEX idx_name ON table_name(col1, col2)
		// Example: CREATE INDEX idx_name ON users(name, age)
		idxOnIdx := strings.Index(sqlU, " ON ")
		if idxOnIdx == -1 {
			return nil, fmt.Errorf("invalid CREATE INDEX syntax")
		}
		index := strings.TrimSpace(sql[12:idxOnIdx])
		onClause := sql[idxOnIdx+4:]
		parenIdx := strings.Index(onClause, "(")
		if parenIdx == -1 || !strings.HasSuffix(onClause, ")") {
			return nil, fmt.Errorf("invalid CREATE INDEX syntax")
		}
		table := strings.TrimSpace(onClause[:parenIdx])
		colsStr := onClause[parenIdx+1 : len(onClause)-1]
		cols := strings.Split(colsStr, ",")
		for i := range cols {
			cols[i] = strings.ToLower(strings.TrimSpace(cols[i]))
		}
		return &CreateIndexStatement{Table: table, Index: index, Columns: cols}, nil
	}
	return nil, fmt.Errorf("unsupported SQL: %s", sql)
}

func parseInsert(sql string) (SQLStatement, error) {
	// INSERT INTO table (col1, col2) VALUES (val1, val2)
	parts := strings.SplitN(sql, "VALUES", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid INSERT syntax")
	}
	left := parts[0]
	right := parts[1]
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	left = strings.TrimPrefix(strings.ToUpper(left), "INSERT INTO ")
	leftParts := strings.SplitN(left, "(", 2)
	table := strings.ToLower(strings.TrimSpace(leftParts[0]))
	cols := strings.Split(strings.TrimSuffix(leftParts[1], ")"), ",")
	for i := range cols {
		cols[i] = strings.ToLower(strings.TrimSpace(cols[i]))
	}
	right = strings.TrimPrefix(right, "(")
	right = strings.TrimSuffix(right, ")")
	vals := strings.Split(right, ",")
	for i := range vals {
		vals[i] = strings.Trim(strings.TrimSpace(vals[i]), "'\"")
	}
	if len(cols) != len(vals) {
		return nil, fmt.Errorf("column/value count mismatch")
	}
	m := make(map[string]string)
	for i := range cols {
		m[cols[i]] = vals[i]
	}
	return &InsertStatement{Table: table, Values: m}, nil
}

func parseSelect(sql string) (SQLStatement, error) {
	// SELECT col1, col2 FROM table WHERE col=val ORDER BY col LIMIT n
	sqlU := strings.ToUpper(sql)
	where := map[string]string{}
	orderBy := ""
	limit := -1
	cols := []string{}
	table := ""
	main := sql
	if idx := strings.Index(sqlU, " WHERE "); idx != -1 {
		main = sql[:idx]
		wherePart := sql[idx+7:]
		if idx2 := strings.Index(strings.ToUpper(wherePart), " ORDER BY "); idx2 != -1 {
			wherePart = wherePart[:idx2]
		}
		if idx2 := strings.Index(strings.ToUpper(wherePart), " LIMIT "); idx2 != -1 {
			wherePart = wherePart[:idx2]
		}
		for _, cond := range strings.Split(wherePart, "AND") {
			cond = strings.TrimSpace(cond)
			if cond == "" {
				continue
			}
			kv := strings.SplitN(cond, "=", 2)
			if len(kv) == 2 {
				where[strings.ToLower(strings.TrimSpace(kv[0]))] = strings.Trim(strings.TrimSpace(kv[1]), "'\"")
			}
		}
	}
	mainU := strings.ToUpper(main)
	main = strings.TrimSpace(main)
	main = strings.TrimPrefix(mainU, "SELECT ")
	main = sql[len("SELECT "):] // get original case
	main = strings.TrimSpace(main)
	fromIdx := strings.Index(strings.ToUpper(main), " FROM ")
	if fromIdx == -1 {
		return nil, fmt.Errorf("missing FROM")
	}
	colPart := main[:fromIdx]
	tablePart := main[fromIdx+6:]
	cols = strings.Split(colPart, ",")
	for i := range cols {
		cols[i] = strings.TrimSpace(cols[i])
	}
	table = strings.ToLower(strings.Fields(tablePart)[0])
	if idx := strings.Index(strings.ToUpper(tablePart), " ORDER BY "); idx != -1 {
		orderBy = strings.Fields(tablePart[idx+9:])[0]
	}
	if idx := strings.Index(strings.ToUpper(tablePart), " LIMIT "); idx != -1 {
		limStr := strings.Fields(tablePart[idx+7:])[0]
		lim, _ := strconv.Atoi(limStr)
		limit = lim
	}
	return &SelectStatement{Table: table, Columns: cols, Where: where, OrderBy: orderBy, Limit: limit}, nil
}

func parseUpdate(sql string) (SQLStatement, error) {
	// UPDATE table SET col=val WHERE col=val
	sqlU := strings.ToUpper(sql)
	setIdx := strings.Index(sqlU, " SET ")
	whereIdx := strings.Index(sqlU, " WHERE ")
	if setIdx == -1 {
		return nil, fmt.Errorf("invalid UPDATE syntax")
	}
	table := strings.ToLower(strings.TrimSpace(sql[6:setIdx]))
	setPart := sql[setIdx+5:]
	where := map[string]string{}
	if whereIdx != -1 {
		setPart = sql[setIdx+5 : whereIdx]
		wherePart := sql[whereIdx+7:]
		for _, cond := range strings.Split(wherePart, "AND") {
			cond = strings.TrimSpace(cond)
			if cond == "" {
				continue
			}
			kv := strings.SplitN(cond, "=", 2)
			if len(kv) == 2 {
				val := strings.TrimSpace(kv[1])
				val = strings.Trim(val, "'\"")
				val = strings.TrimSpace(val)
				where[strings.ToLower(strings.TrimSpace(kv[0]))] = val
			}
		}
	}
	set := map[string]string{}
	for _, assign := range strings.Split(setPart, ",") {
		assign = strings.TrimSpace(assign)
		if assign == "" {
			continue
		}
		kv := strings.SplitN(assign, "=", 2)
		if len(kv) == 2 {
			val := strings.TrimSpace(kv[1])
			val = strings.Trim(val, "'\"")
			val = strings.TrimSpace(val)
			set[strings.ToLower(strings.TrimSpace(kv[0]))] = val
		}
	}
	return &UpdateStatement{Table: table, Set: set, Where: where}, nil
}

func parseDelete(sql string) (SQLStatement, error) {
	// DELETE FROM table WHERE col=val
	sqlU := strings.ToUpper(sql)
	where := map[string]string{}
	table := ""
	tablePart := sql[12:]
	table = strings.ToLower(strings.Fields(tablePart)[0])
	if idx := strings.Index(sqlU, " WHERE "); idx != -1 {
		wherePart := sql[idx+7:]
		for _, cond := range strings.Split(wherePart, "AND") {
			cond = strings.TrimSpace(cond)
			if cond == "" {
				continue
			}
			kv := strings.SplitN(cond, "=", 2)
			if len(kv) == 2 {
				where[strings.ToLower(strings.TrimSpace(kv[0]))] = strings.Trim(strings.TrimSpace(kv[1]), "'\"")
			}
		}
	}
	return &DeleteStatement{Table: table, Where: where}, nil
}
