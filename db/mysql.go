package db

import (
	"encoding/hex"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var Db *sqlx.DB

func InitDB() {
	database, err := sqlx.Open("mysql", "root:112112@tcp(127.0.0.1:3306)/mini_dfs")
	if err != nil {
		fmt.Println("open mysql failed,", err)
		return
	}
	Db = database
}

func InsertFile(filename string) int64 {
	var id int64
	err := Db.QueryRow("select id from file where filename=?", filename).Scan(&id)
	if err == nil {
		return id
	}

	r, err := Db.Exec("insert into file(filename)values(?)", filename)
	if err != nil {
		fmt.Println("exec failed, ", err)
		return -1
	}
	id, err = r.LastInsertId()
	if err != nil {
		fmt.Println("exec failed, ", err)
		return -1
	}
	fmt.Println("insert succ:", id)
	return id
}

func InsertChunk(fileid int64, datanodeList string) int64 {
	r, err := Db.Exec("insert into chunk(fileid, datanode_id)values(?,?)", fileid, datanodeList)
	if err != nil {
		fmt.Println("exec failed, ", err)
		return -1
	}
	id, err := r.LastInsertId()
	if err != nil {
		fmt.Println("exec failed, ", err)
		return -1
	}
	fmt.Println("insert succ:", id)
	return id
}

func UpdateChunk(chunkId int64, md5code []byte) {
	r, err := Db.Exec("update chunk set md5=? where id=?", hex.EncodeToString(md5code), chunkId)
	if err != nil {
		fmt.Println("exec failed, ", err)
		return
	}
	n, err := r.RowsAffected() // 操作影响的行数
	if err != nil {
		fmt.Printf("get RowsAffected failed, err:%v\n", err)
		return
	}
	fmt.Printf("update success, affected rows:%d\n", n)
}
