// Copyright 2020 The Author Elias Mei. All rights reserved.
// Use of this source code is governed by a BSD-style

// Simple CURD for mysql
// Author	: 	Elias Mei
// Email	: 	mylicharm@gmail.com
// Version	: 	0.1
// Usage	:
// 		step 1, defined a model struct, eg:
//			type Task struct {
//				id       int64
//				name     string
//				url      string
//				count    int32
//				valid    bool
//				createAt int64
//			}
//		step 2, insert one record
// 			task := Task{1, "test", "url", 33, true, time.Now().Unix()}
//			db.Insert(task)
// 		step 3, update one record by id
// 			task.url = "new url"
//			db.Update(task)
//		step 4, select one record
//			task, err := db.GetQueryBuilder().Select(&Task{}).Where("name", "test").GetOne()
//		step 5, select many records
// 			arr, err := db.GetQueryBuilder().Select(&Task{}).Where("name", "test").GetMany()
//			task1 := arr[0].(*Task)  // *interface{} to *Task
// 		step 6, delete one record by id
//			task := Task{}
//			task.id = 1
//			db.Delete(task)

package golibs

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"strings"
	"unsafe"
)

type DbConfig struct {
	UserName string
	Password string
	Host     string
	Port     string
	DbName   string
}

var logger = new(Logger)

// Db connection pool
var DB *sql.DB

// 方法名大写 == public
func InitDB(c *DbConfig) {
	logger.INFO("starting to connect to db server...")
	// 构建连接字符串
	path := strings.Join(
		[]string{c.UserName, ":", c.Password, "@tcp(", c.Host, ":", c.Port, ")/", c.DbName, "?charset=utf8"},
		"")
	// 建立数据库连接
	DB, _ = sql.Open("mysql", path)

	// 设置数据库连接存活时间
	DB.SetConnMaxLifetime(100)
	// 设置最大闲置连接数
	DB.SetMaxIdleConns(2)
	// 设置最大连接数
	DB.SetMaxOpenConns(5)
	// 验证连接
	if err := DB.Ping(); err != nil {
		logger.ERROR("connect to db failed, uri: %v , error: %v", path, err)
		return
	}
	logger.INFO("DB connected. %v ", path)
}

// 插入一条记录
// 返回记录的id
func Insert(st interface{}) int64 {
	var sqlStr, values, err = buildInsertSql(st)
	if err != nil {
		logger.ERROR("%v", err.Error())
		return -1
	}
	logger.DEBUG(sqlStr)

	res, err := sqlExec(sqlStr, values)
	if err != nil {
		logger.Error(err)
		return -1
	}

	index, _ := res.LastInsertId()
	logger.INFO("Insert successfully, id: %v", index)
	return index
}

// 根据id更新一条记录
// 返回影响的条数
func Update(st interface{}) int64 {
	sqlStr, values, err := buildUpdateSql(st)
	if err != nil {
		logger.ERROR("%v", err.Error())
		return 0
	}
	logger.DEBUG(sqlStr)

	res, err := sqlExec(sqlStr, values)
	if err != nil {
		logger.Error(err)
		return 0
	}
	rows, err := res.RowsAffected()
	logger.INFO("Update successfully, affected rows: %v", rows)

	return rows
}

// 根据id删除一条记录
// 返回删除的条数
func Delete(st interface{}) int64 {
	sqlStr, values, err := buildDeleteSql(st)
	if err != nil {
		logger.ERROR("%v", err.Error())
		return 0
	}
	logger.INFO(sqlStr)

	res, err := sqlExec(sqlStr, values)
	if err != nil {
		logger.Error(err)
		return 0
	}
	rows, err := res.RowsAffected()
	logger.INFO("Delete successfully, deleted rows: %v", rows)

	return rows
}

// 查询语句构造
type QueryBuilder struct {
	Target    interface{}
	tableName string
	typ       reflect.Type
	where     string // 查询条件
	values    []interface{}
}

func GetQueryBuilder() *QueryBuilder {
	q := new(QueryBuilder)
	q.where = ""
	return q
}

func (q *QueryBuilder) Select(st interface{}) *QueryBuilder {
	t := reflect.TypeOf(st)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// QueryBuilder 初始化
	q.Target = st
	q.typ = t
	name, _ := firstCharToLower(t.Name())
	q.tableName = name
	return q
}
func (q *QueryBuilder) Sql(sql string, values ...interface{}) *QueryBuilder {
	q.where = q.where + sql
	q.values = append(q.values, values...)
	return q
}
func (q *QueryBuilder) Where(name string, value interface{}) *QueryBuilder {
	q.where = q.where + " " + name + " = ? "
	q.values = append(q.values, value)
	return q
}
func (q *QueryBuilder) And(name string, value interface{}) *QueryBuilder {
	q.where = q.where + " AND " + name + " = ? "
	q.values = append(q.values, value)
	return q
}
func (q *QueryBuilder) Or(name string, value interface{}) *QueryBuilder {
	q.where = q.where + " OR " + name + " = ? "
	q.values = append(q.values, value)
	return q
}

func (q *QueryBuilder) GetOne() (interface{}, error) {
	fields := getFieldsArray(q.Target)
	query := "SELECT *  FROM `" + q.tableName + "` WHERE " + q.where + " LIMIT 1"
	logger.DEBUG(query)
	err := DB.QueryRow(query, q.values...).Scan(fields...)

	if err != nil {
		// logger.Error(err)
		return q.Target, err
	}
	return q.Target, nil
}

func (q *QueryBuilder) GetMany() ([]interface{}, error) {
	query := "SELECT *  FROM `" + q.tableName + "` WHERE " + q.where
	logger.DEBUG(query)
	rows, err := DB.Query(query, q.values...)
	if err != nil {
		// logger.Error(err)
		return nil, err
	}
	var arr []interface{}
	for rows.Next() {
		obj := reflect.New(q.typ).Interface()
		fields := getFieldsArray(obj)
		err := rows.Scan(fields...)
		if err != nil {
			logger.Error(err)
			continue
		}
		arr = append(arr, obj)
	}
	return arr, nil
}

func getFieldsArray(q interface{}) []interface{} {
	t := reflect.TypeOf(q)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	v := reflect.ValueOf(q)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	var field []interface{}

	fieldNum := t.NumField()
	for i := 0; i < fieldNum; i++ {
		//name := t.Field(i).Name
		value := v.Field(i)
		pointer := getPtrByType(value)
		field = append(field, pointer)
	}
	return field
}

// Build insert sql string
func buildInsertSql(st interface{}) (string, []interface{}, error) {
	t := reflect.TypeOf(st)
	table, _ := firstCharToLower(t.Name())
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		//logger.ERROR("Param type is not Struct")
		return "", nil, errors.New("param type is not Struct")
	}
	var names = "("
	var questionMarks = "("
	var values []interface{}
	fieldNum := t.NumField()
	// 反射获取值的集合
	v := reflect.ValueOf(st)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	for i := 0; i < fieldNum; i++ {
		name, _ := firstCharToLower(t.Field(i).Name)
		if name != "id" {
			value := checkStructFieldType(v.Field(i))
			names = names + name + ","
			questionMarks = questionMarks + "?,"
			values = append(values, value)
		}
	}

	names = names[0:len(names)-1] + ")"
	questionMarks = questionMarks[0:len(questionMarks)-1] + ")"
	sqlStr := "INSERT INTO `" + table + "` " + names + " VALUES " + questionMarks
	return sqlStr, values, nil
}

// 构建更新语句
func buildUpdateSql(st interface{}) (string, []interface{}, error) {
	t := reflect.TypeOf(st)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	table, _ := firstCharToLower(t.Name())
	if t.Kind() != reflect.Struct {
		//logger.ERROR("Param type is not Struct")
		return "", nil, errors.New("param type is not Struct")
	}
	var sets = ""
	var values []interface{}
	fieldNum := t.NumField()
	var id int64
	// 反射获取值的集合
	v := reflect.ValueOf(st)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	for i := 0; i < fieldNum; i++ {
		name, _ := firstCharToLower(t.Field(i).Name)
		sets = sets + name + "=?,"
		value := v.Field(i)
		values = append(values, checkStructFieldType(value))
		if name == "id" {
			id = value.Int()
		}
	}
	values = append(values, id)
	sets = sets[0 : len(sets)-1]
	sqlStr := "UPDATE " + table + " SET " + sets + " WHERE id = ?"
	return sqlStr, values, nil
}

// 构建删除语句
func buildDeleteSql(st interface{}) (string, []interface{}, error) {
	t := reflect.TypeOf(st)
	table, _ := firstCharToLower(t.Name())
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		//logger.ERROR("Param type is not Struct")
		return "", nil, errors.New("param type is not Struct")
	}

	var values []interface{}
	fieldNum := t.NumField()
	// 反射获取值的集合
	v := reflect.ValueOf(st)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	for i := 0; i < fieldNum; i++ {
		name, _ := firstCharToLower(t.Field(i).Name)
		value := v.FieldByName(name)
		if name == "id" {
			values = append(values, checkStructFieldType(value))
		}
	}

	sqlStr := "DELETE FROM " + table + " WHERE id = ?"
	return sqlStr, values, nil
}

// 实现sql.Result接口 执行出错
type SqlExecErrorResult int64

func (result SqlExecErrorResult) LastInsertId() (int64, error) {
	return -1, errors.New("sql exec error")
}

func (result SqlExecErrorResult) RowsAffected() (int64, error) {
	return -1, errors.New("sql exec error")
}

// 执行sql语句
func sqlExec(sqlStr string, values []interface{}) (sql.Result, error) {
	// 开启事务
	tx, err := DB.Begin()
	if err != nil {
		//logger.ERROR("Open database transaction failed, error: %v", err.Error())
		return SqlExecErrorResult(-1), errors.New(fmt.Sprintf("open database transaction failed, error: %v", err.Error()))
	}
	// sql预编译
	stmt, err := tx.Prepare(sqlStr)
	if err != nil {
		//logger.ERROR("Sql Prepare failed, error: %v", err.Error())
		return SqlExecErrorResult(-1), errors.New(fmt.Sprintf("sql Prepare failed, error: %v", err.Error()))
	}
	res, err := stmt.Exec(values...)
	if err != nil {
		//logger.ERROR("Sql exec failed, error: %v", err.Error())
		return SqlExecErrorResult(-1), errors.New(fmt.Sprintf("sql exec failed, error: %v", err.Error()))
	}
	// 提交事务
	tx.Commit()
	return res, nil
}

func checkStructFieldType(i reflect.Value) interface{} {
	//if !i.IsValid() {
	//	return nil
	//}
	switch i.Kind() {
	case reflect.String:
		return i.String()
	case reflect.Int8:
		return i.Int()
	case reflect.Int16:
		return i.Int()
	case reflect.Int32:
		return i.Int()
	case reflect.Int64:
		return i.Int()
	case reflect.Float32:
		return i.Float()
	case reflect.Float64:
		return i.Float()
	case reflect.Bool:
		return i.Bool()
	default:
		return i.String()
	}
}

func getPtrByType(i reflect.Value) interface{} {
	//if !i.IsValid() {
	//	return nil
	//}
	switch i.Kind() {
	case reflect.String:
		return (*string)(unsafe.Pointer(i.Addr().Pointer()))
	case reflect.Int8:
		return (*int8)(unsafe.Pointer(i.Addr().Pointer()))
	case reflect.Int16:
		return (*int16)(unsafe.Pointer(i.Addr().Pointer()))
	case reflect.Int32:
		return (*int32)(unsafe.Pointer(i.Addr().Pointer()))
	case reflect.Int64:
		return (*int64)(unsafe.Pointer(i.Addr().Pointer()))
	case reflect.Float32:
		return (*float32)(unsafe.Pointer(i.Addr().Pointer()))
	case reflect.Float64:
		return (*float64)(unsafe.Pointer(i.Addr().Pointer()))
	case reflect.Bool:
		return (*bool)(unsafe.Pointer(i.Addr().Pointer()))
	default:
		return (*string)(unsafe.Pointer(i.Addr().Pointer()))
	}
}

func firstCharToLower(name string) (string, error) {
	lens := len(name)
	if lens < 1 {
		return "", errors.New(fmt.Sprintf("error name:"))
	} else {
		return strings.ToLower(name[0:1]) + name[1:], nil
	}
}
