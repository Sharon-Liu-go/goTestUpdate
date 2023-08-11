package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

type ID struct {
	FirstId int
	LastId  int
}

type RangeID struct {
	FromId int
	ToId   int
	LastId int
}

type PlayerInfo struct {
	Account string
	Info    sql.NullString // Use sql.NullString to handle NULL values for the "Info" column
	Games   sql.NullString // Use sql.NullString to handle NULL values for the "Games" column
	Rooms   sql.NullString // Use sql.NullString to handle NULL values for the "rooms" column
	Owner   sql.NullString // Use sql.NullString to handle NULL values for the "Owner" column
}

var goroutineNum = 8
var batchSize = 1000
var countInsertRows int

func getFistAndLastId(db *sql.DB) (ID, error) {
	var id ID
	err := db.QueryRow("SELECT MIN(id) as firstId, MAX(id) as lastId FROM "+os.Getenv("ACCOUNT_SOURCE_TABLE")).Scan(&id.FirstId, &id.LastId)
	if err != nil {
		return id, err
	}
	fmt.Println(id)
	return id, nil
}

func getBatchData(id int, rangeId *RangeID, wg *sync.WaitGroup, db *sql.DB, stmt *sql.Stmt) {
	startTime := time.Now()
	// Create a new ID struct for this goroutine
	localRangeId := RangeID{
		FromId: rangeId.FromId,
		ToId:   rangeId.ToId,
		LastId: rangeId.LastId,
	}
	// 执行查询
	startTime_select := time.Now()
	rows, err := stmt.Query(localRangeId.FromId, localRangeId.ToId)
	if err != nil {
		panic("SELECT执行查询失败：" + err.Error())
	}
	defer rows.Close()
	endTime_select := time.Now()
	duration_select := endTime_select.Sub(startTime_select)

	startTime_cov := time.Now()
	var playerInfo PlayerInfo
	var data []interface{}
	var countRows int
	for rows.Next() {
		countRows++
		err := rows.Scan(&playerInfo.Account, &playerInfo.Info, &playerInfo.Games, &playerInfo.Rooms, &playerInfo.Owner)
		if err != nil {
			log.Fatal("读取查询结果失败：", err)
		}
		//轉格式
		var strGames []byte
		var strRooms []byte
		var strOwner []byte

		// Convert the JSON string to a map[string]interface{}
		if playerInfo.Games.Valid {
			var jsonGames map[string]interface{}
			err_jsonGames := json.Unmarshal([]byte(playerInfo.Games.String), &jsonGames)
			if err_jsonGames != nil {
				log.Fatal("JSON unmarshal error:", err_jsonGames)
			}
			// Modify the keys with "_CNY" suffix
			games := make(map[string]interface{})
			for key, value := range jsonGames {
				games[key] = value
				games[key+"_CNY"] = value
			}
			// Convert the modified map back to a JSON string
			strGames, _ = json.Marshal(games)
		}
		if playerInfo.Rooms.Valid {
			var jsonRooms map[string]interface{}
			err_jsonRooms := json.Unmarshal([]byte(playerInfo.Rooms.String), &jsonRooms)
			if err_jsonRooms != nil {
				log.Fatal("JSON unmarshal error:", err_jsonRooms)
			}
			// Modify the keys with "_CNY" suffix
			rooms := make(map[string]interface{}) //要先初始化
			for key, value := range jsonRooms {
				rooms[key] = value
				rooms[key+"_CNY"] = value
			}
			// Convert the modified map back to a JSON string
			strRooms, _ = json.Marshal(rooms)
		}

		if playerInfo.Owner.Valid {
			var jsonOwner map[string]interface{}
			err_jsonOwner := json.Unmarshal([]byte(playerInfo.Owner.String), &jsonOwner)
			if err_jsonOwner != nil {
				log.Fatal("JSON unmarshal error:", err_jsonOwner)
			}
			// Modify the keys with "_CNY" suffix
			owner := map[string]interface{}{
				"owner":     jsonOwner,
				"owner_CNY": jsonOwner,
			}
			// Convert the modified map back to a JSON string
			strOwner, _ = json.Marshal(owner)
		}

		data = append(data, playerInfo.Account, playerInfo.Info, string(strGames), string(strRooms), string(strOwner))

		insertSize := 1000
		if countRows%insertSize == 0 {
			fmt.Println("批量寫入:", "id", id, "countRows", countRows)
			countRows -= insertSize
			batchData := data
			data = []interface{}{}
			insertDB(id, insertSize, db, batchData)
		}
	}
	endTime_cov := time.Now()
	duration_cov := endTime_cov.Sub(startTime_cov)
	if err := rows.Err(); err != nil {
		log.Fatal("遍历结果集发生错误：", err)
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	fmt.Println("getBatchData id:", id, "FromId: ", localRangeId.FromId, "ToId: ", localRangeId.ToId, "countRows", countRows, "duration_select:  ", duration_select, "duration_cov + insertDB: ", duration_cov, "代码执行时间：", duration)

	if countRows > 0 {
		//批次insert:
		insertDB(id, countRows, db, data)
	}

	if (localRangeId.ToId + (batchSize*(goroutineNum-1) + 1)) > localRangeId.LastId {
		fmt.Println("*****Done*****")
		wg.Done()
		return
	}
	localRangeId.FromId += batchSize * goroutineNum
	localRangeId.ToId = localRangeId.FromId + batchSize
	getBatchData(id, &localRangeId, wg, db, stmt)

}

func insertDB(id int, countRows int, db *sql.DB, data []interface{}) {
	countInsertRows += countRows
	// 准备查询语句
	startTime := time.Now()
	sql := "INSERT INTO All_transform_result.playerInfos_update (account,info,games,rooms,owner) VALUES (?,?,?,?,?)"
	sql += strings.Repeat(",(?,?,?,?,?)", countRows-1)
	sql += "  ON DUPLICATE KEY UPDATE info = VALUES(info), games = VALUES(games), rooms = VALUES(rooms),owner = VALUES(owner);"
	stmt, err := db.Prepare(sql)
	if err != nil {
		panic("插入准备查询语句失败：" + err.Error())
	}
	defer stmt.Close()
	_, err_Exec := stmt.Exec(data...)
	if err_Exec != nil {
		log.Fatal("插入执行失败：", err_Exec)
	}
	endTime := time.Now()
	duration := endTime.Sub(startTime)
	fmt.Println("id", id, "countRows", countRows, "執行時間：", duration)
}

func main() {
	startTime_all := time.Now()
	// 載入 .env 檔案中的變數
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	// 数据库连接信息
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASS")
	dbName := "game_record"

	// 构建连接字符串
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPass, dbHost, dbPort, dbName)

	// 连接到 MySQL 数据库
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	// 設定連線池的最大打開連線數和最大閒置連線數
	db.SetMaxOpenConns(100) // 最大打開連線數
	db.SetMaxIdleConns(15)  // 最大閒置連線數

	// 与数据库建立连接
	err = db.Ping()
	if err != nil {
		panic("无法连接到数据库：" + err.Error())
	}

	fmt.Println("成功连接到 MySQL 数据库！")

	//依據執行行為，建立批次查询的準備語句
	var sql_getBatchData string
	switch os.Getenv("ACTION") {
	case "PRE_INSERT":
		fmt.Println("ACTION:PRE_INSERT")
		sql_getBatchData = "SELECT a.account,info,games,rooms,owner FROM " + os.Getenv("ACCOUNT_SOURCE_TABLE") + " a INNER JOIN " + os.Getenv("PLAYERINFOS_SOURCE_TABLE") + " b on a.account = b.account WHERE a.id >=? AND a.id <? "

	case "UPDATE_BY_LASTLOGINTIME":
		startLastloginTime := os.Getenv("START_LASTLOGINTIME")
		fmt.Println("ACTION:UPDATE_BY_LASTLOGINTIME", "START_LASTLOGINTIME:", startLastloginTime)
		sql_getBatchData = "SELECT a.account,info,games,rooms,owner FROM " + os.Getenv("ACCOUNT_SOURCE_TABLE") + " a INNER JOIN " + os.Getenv("PLAYERINFOS_SOURCE_TABLE") + " b on a.account = b.account WHERE a.id >=? AND a.id <? AND a.lastlogintime >= '" + startLastloginTime + "'"

	default:
		fmt.Println("請提供ACTION: PRE_INSERT 或 UPDATE_BY_LASTLOGINTIME")
	}

	stmt_getBatchData, err := db.Prepare(sql_getBatchData)
	if err != nil {
		panic("准备查询语句失败：" + err.Error())
	}
	defer stmt_getBatchData.Close()

	//取得玩家最小id和最大id
	startTime_getId := time.Now()
	ids, err := getFistAndLastId(db)
	if err != nil {
		panic("查詢game_api.account的最小id和最大id失敗：" + err.Error())
	}
	endTime_getId := time.Now()
	duration_getId := endTime_getId.Sub(startTime_getId)
	fmt.Printf("查询结果：\nFirstId: %d\nLastId: %d\n", ids.FirstId, ids.LastId)
	fmt.Println("代码执行时间：", duration_getId)

	// 使用 WaitGroup 来等待所有 Goroutine 完成
	var wg sync.WaitGroup

	// 启动 8 个 Goroutine 来查询数据
	startTime_goroutine := time.Now()
	for i := 0; i < goroutineNum; i++ {
		wg.Add(1)
		firstId := ids.FirstId + (batchSize * i)
		lastId := ids.LastId
		fmt.Println(i, firstId, lastId)
		rangeId := RangeID{
			FromId: firstId,
			ToId:   firstId + batchSize,
			LastId: lastId,
		}
		go getBatchData(i, &rangeId, &wg, db, stmt_getBatchData)
	}

	// 等待所有 Goroutine 完成
	wg.Wait()
	endTime_goroutine := time.Now()
	duration_goroutine := endTime_goroutine.Sub(startTime_goroutine)
	endTime_all := time.Now()
	duration_all := endTime_all.Sub(startTime_all)
	fmt.Println("goroutineNum", goroutineNum, "每次select", batchSize, "筆 insertSize 1000筆")
	fmt.Println("- get account min id & max id 执行时间：", duration_getId)
	fmt.Println("- select & convert & insert 执行时间：", duration_goroutine)
	fmt.Println("總执行时间:", duration_all)
	fmt.Println("總affected rows:", countInsertRows)
}
