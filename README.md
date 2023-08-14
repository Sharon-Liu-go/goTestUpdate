# playerInfos數據轉移工具

## 目的
將 ***Mysql*** 的 ***game_record.playerInfos*** 轉換成多幣種新格式並寫進目標table。

### 執行種類
    1. 提前資料遷徙:於停機當天之前的某一日，先進行全部玩家之資料遷徙  
    2. 停機後的資料遷徙:根據玩家的最後登陸時間，只更新提前日之後(含提前日)的玩家數據


## 環境安裝
### 使用 Go 语言

確保已經正確安裝了 Go 編程環境。
如果尚未安裝 Go，可以按照官方文檔的說明進行安裝：[安装 Go](https://golang.org/doc/install)


## 開始使用

### 🛠 原始碼建置

1. 下載程式碼

   ```shell
   git clone "https://github.com/Sharon-Liu-go/goTestUpdate.git" playerInfoTransform
   cd playerInfoTransform
   ```

2. 從原始碼建置工具

   ```shell
   go build -o toolname main.go

   //toolname 可自行工具命名
   ```

3. 執行前確認  
    [v] 請確認來源table和欲存入之table皆已存在新DB  
    [v] 於檔案中的.env檔，完成變數設定  
    
   | 參數                     | 描述                                                                                                                  |
   | ------------------------ | --------------------------------------------------------------------------------------------------------------------- |
   | DB_HOST                  | 連接到MySQL伺服器的IP地址。                                                                                           |
   | DB_PORT                  | 連接到MySQL伺服器的埠號。                                                                                             |
   | DB_USER                  | 連接到MySQL伺服器的使用者名稱。                                                                                       |
   | DB_PASS                  | 連接到MySQL伺服器的密碼。                                                                                             |
   | ACTION                   | 執行行為: PRE_INSERT (提前資料遷徙) 或 UPDATE_BY_LASTLOGINTIME(停機後的資料遷徙)                                      |
   | START_LASTLOGINTIME      | 提前日，格式舉例:'2023-08-08 00:00:00'。<br>若ACTION為UPDATE_BY_LASTLOGINTIME為必填，PRE_INSERT則有填與不填皆不影響。 |
   | ACCOUNT_SOURCE_TABLE     | 玩家TABLE。                                                                                                           |
   | PLAYERINFOS_SOURCE_TABLE | 要轉格式的playerinfos DB.TABLE。                                                                                      |
   | PLAYERINFOS_INTO_TABLE   | 要存入轉格式後的playerinfos DB.TABLE。                                                                                |


4. 執行
   ```shell
   ./toolname
   ```

### 遷移完成console印出
   
    (範例:)
    goroutineNum 8 每次select 1000 筆 insertSize 1000筆
    - get account min id & max id 执行时间： 1.599ms
    - select & convert & insert 执行时间： 15m26.0617591s
    總执行时间: 15m26.0664879s
    總affected rows: 5042216
   