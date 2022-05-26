package db

import (
	"encoding/hex"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"log"
	"os"
	"strconv"
	"strings"
)

var sql_logger = log.New(os.Stdout, "SQL:", log.Lshortfile)
var GLOBAL_DB *sqlx.DB

type ChunkDB struct {
	Id            int64  `db:"id"`
	FileId        string `db:"file_id"`
	MD5CODE       string `db:"md5"`
	DataNodeAddrs string `db:"datanode_addrs"`
}

// 初始化全局的数据库连接对象
func InitDB() {
	database, err := sqlx.Open("mysql", "root:root@tcp(127.0.0.1:3306)/mini_dfs")
	if err != nil {
		sql_logger.Println("open mysql failed,", err)
		return
	}
	GLOBAL_DB = database
}

func InsertFile(filename string, chunk_num int64) int64 {

	r, err := GLOBAL_DB.Exec("insert into file(filename,chunk_num)values(?,?)", filename, chunk_num)
	if err != nil {
		sql_logger.Println("insert file exec failed, ", err)
		return -1
	}
	id, err := r.LastInsertId()
	if err != nil {
		sql_logger.Println("insert file exec failed, ", err)
		return -1
	}
	sql_logger.Printf("insert file:%d succ:\n", id)
	return id
}

func InsertChunk(fileid int64, datanodeList string) int64 {
	r, err := GLOBAL_DB.Exec("insert into chunk(file_id, datanode_addrs)values(?,?)", fileid, datanodeList)
	if err != nil {
		sql_logger.Println("insert chunk exec failed, ", err)
		return -1
	}
	id, err := r.LastInsertId()
	if err != nil {
		sql_logger.Println("insert chunk exec failed, ", err)
		return -1
	}
	sql_logger.Printf("insert chunk:%d succ\n", id)
	return id
}

func InsertChunk2Node(chunkid int64, datanodeAddr string) {
	_, err := GLOBAL_DB.Exec("insert into chunk2node(chunk_id, datanode)values(?,?)", chunkid, datanodeAddr)
	if err != nil {
		sql_logger.Printf("insert chunk%d exec failed for: %v", chunkid, err)
	}
	return
}

func QueryFile(filename string) []ChunkDB {
	var id int64
	err := GLOBAL_DB.QueryRow("select id from file where filename=?", filename).Scan(&id)
	if err != nil {
		sql_logger.Printf("file %s not exist for: %v\n", filename, err)
		return nil
	}
	var chunks []ChunkDB
	err = GLOBAL_DB.Select(&chunks, "select id, md5, datanode_addrs from chunk where file_id=?", id)
	if err != nil {
		sql_logger.Printf("get chunk failed for: %v\n", err)
		return nil
	}
	return chunks
}

func QueryChunks(chunkId []int64) []ChunkDB {
	var chunks []ChunkDB

	var s []string
	for i := 0; i < len(chunkId); i++ {
		s = append(s, strconv.FormatInt(chunkId[i], 10)) //把每个元素都变成string类型
	}

	var ss string
	ss = strings.Join(s, "','") //这里填入的s必须要为string类型的数组，所以前面要转换成string类型
	//此时的ss为：1','2','3','4','5','6','7

	query := fmt.Sprintf("select id,md5,datanode_addrs from chunk where id in ('%s')", ss)
	//组合之后：('1','2','3','4','5','6','7')
	result, err := GLOBAL_DB.Query(query)

	if err != nil {
		sql_logger.Printf("get chunk failed for: %v\n", err)
		return nil
	}
	for result.Next() {
		chunkDB := ChunkDB{}
		result.Scan(&chunkDB.Id, &chunkDB.MD5CODE, &chunkDB.DataNodeAddrs)
		chunks = append(chunks, chunkDB)
	}
	return chunks
}

func QueryChunkOnDataNode(addr string) []int64 {
	var id []int64
	err := GLOBAL_DB.Select(&id, "select chunk_id from chunk2node where datanode=?", addr)
	if err != nil {
		sql_logger.Printf("query chunk on %s failed for: %v\n", addr, err)
		return nil
	}
	return id
}

func DeleteChunkOnDataNode(addr string) {
	res, err := GLOBAL_DB.Exec("delete from chunk2node where datanode=?", addr)
	if err != nil {
		sql_logger.Println("exec failed, ", err)
		return
	}

	row, err := res.RowsAffected()
	if err != nil {
		sql_logger.Println("rows failed, ", err)
	}
	sql_logger.Println("delete succ: ", row)
}

func UpdateChunk(chunkId int64, md5code []byte) {
	r, err := GLOBAL_DB.Exec("update chunk set md5=? where id=?", hex.EncodeToString(md5code), chunkId)
	if err != nil {
		sql_logger.Println("exec failed, ", err)
		return
	}
	n, err := r.RowsAffected() // 操作影响的行数
	if err != nil {
		sql_logger.Printf("get RowsAffected failed, err:%v\n", err)
		return
	}
	sql_logger.Printf("update success, affected rows:%d\n", n)
}

func UpdateChunkDataNode(chunkId int64, datanodeAddrs string) {
	r, err := GLOBAL_DB.Exec("update chunk set datanode_addrs=? where id=?", datanodeAddrs, chunkId)
	if err != nil {
		sql_logger.Println("exec failed, ", err)
		return
	}
	n, err := r.RowsAffected() // 操作影响的行数
	if err != nil {
		sql_logger.Printf("get RowsAffected failed, err:%v\n", err)
		return
	}
	sql_logger.Printf("update success, affected rows:%d\n", n)
}
