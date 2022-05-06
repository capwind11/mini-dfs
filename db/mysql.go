package db

import (
	"encoding/hex"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var Db *sqlx.DB

type ChunkDB struct {
	Id             int64  `db:"Id"`
	file_id        string `db:"file_id"`
	MD5CODE        string `db:"md5"`
	DATANODE_ADRRS string `db:"datanode_addrs"`
}

func InitDB() {
	database, err := sqlx.Open("mysql", "root:112112@tcp(127.0.0.1:3306)/mini_dfs")
	if err != nil {
		fmt.Println("open mysql failed,", err)
		return
	}
	Db = database
}

func InsertFile(filename string, chunk_num int64) int64 {

	r, err := Db.Exec("insert into file(filename,chunk_num)values(?,?)", filename, chunk_num)
	if err != nil {
		fmt.Println("insert file exec failed, ", err)
		return -1
	}
	id, err := r.LastInsertId()
	if err != nil {
		fmt.Println("insert file exec failed, ", err)
		return -1
	}
	fmt.Printf("insert file:%d succ:\n", id)
	return id
}

func InsertChunk(fileid int64, datanodeList string) int64 {
	r, err := Db.Exec("insert into chunk(file_id, datanode_addrs)values(?,?)", fileid, datanodeList)
	if err != nil {
		fmt.Println("insert chunk exec failed, ", err)
		return -1
	}
	id, err := r.LastInsertId()
	if err != nil {
		fmt.Println("insert chunk exec failed, ", err)
		return -1
	}
	fmt.Printf("insert chunk:%d succ\n", id)
	return id
}

func InsertChunk2Node(chunkid int64, datanodeAddr string) {
	_, err := Db.Exec("insert into chunk2node(chunk_id, datanode)values(?,?)", chunkid, datanodeAddr)
	if err != nil {
		fmt.Println("insert chunk exec failed, ", err)
	}
	return
}

func QueryFile(filename string) []ChunkDB {
	var id int64
	err := Db.QueryRow("select Id from file where filename=?", filename).Scan(&id)
	if err != nil {
		fmt.Printf("file %s not exist for: %v\n", filename, err)
		return nil
	}
	var chunks []ChunkDB
	err = Db.Select(&chunks, "select Id, md5, datanode_addrs from chunk where file_id=?", id)
	if err != nil {
		fmt.Printf("get chunk failed for: %v\n", err)
		return nil
	}
	return chunks
}

func UpdateChunk(chunkId int64, md5code []byte) {
	r, err := Db.Exec("update chunk set md5=? where Id=?", hex.EncodeToString(md5code), chunkId)
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
