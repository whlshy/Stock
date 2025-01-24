import requests
from bs4 import BeautifulSoup
import json
import time
import os
import re
from datetime import datetime
import pyodbc

# 設定日誌文件名稱
log_file = './log'

def write_log(log_message):
    # 取得當前時間
    now = datetime.now()

    # 格式化時間為 yyyy-mm-dd HH:MM
    formatted_time = now.strftime('%Y-%m-%d %H:%M')

    log_path = f'{log_file}/{now.strftime('%Y-%m-%d')}_log.txt'
    message = f'{formatted_time} - {log_message}\n'

    # 檢查文件是否存在
    if not os.path.exists(log_path):
        create_dir(log_path)
        # 如果文件不存在，創建文件並寫入日誌訊息
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(message)
    else:
        # 如果文件存在，追加寫入日誌訊息
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(message)

# 取得某天股價資料
def get_date_data(date_str):  # date_str = "YYYYMMDD"
    year, month, day = get_date_info(date_str)

    # listed_stock = 上市, otc_market = 上櫃
    data = {
        "listed_stock": None,
        "otc_market": None,
    }

    # 讀取上市資料
    try:
        with open(
            f"./data/Listed_Stock/{year}/{month}/{date_str}_listed.json",
            "r",
            encoding="utf-16",
        ) as f:
            read_data = f.read()
            # 如果檔案是空的，則回傳None (如果上市是空的，上櫃也是空的)
            if len(read_data) == 0:
                data["listed_stock"] = None
            else:
                data["listed_stock"] = json.loads(read_data)
    except FileNotFoundError:
        read_data = get_listed_stock_data(date_str)
        if len(read_data) == 0:
            data["listed_stock"] = None
        else:
            data["listed_stock"] = read_data

    # 讀取櫃買資料
    try:
        with open(
            f"./data/OTC_Market/{year}/{month}/{date_str}_otc.json",
            "r",
            encoding="utf-16",
        ) as f:
            read_data = f.read()
            if len(read_data) == 0:
                data["otc_market"] = None
            else:
                data["otc_market"] = json.loads(read_data)
    except FileNotFoundError:
        read_data = get_otc_market_data(date_str)
        if len(read_data) == 0:
            data["otc_market"] = None
        else:
            data["otc_market"] = read_data

    return data

# 確認目錄是否存在，不存在則建立
def create_dir(file_path):
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)  # 建立多層目錄

# 把日期字串拆解成年、月、日
def get_date_info(date_str):
    match = re.match(r"(\d+)(\d{2})(\d{2})", date_str)

    year = match.group(1)  # 年份
    month = match.group(2)  # 月份
    day = match.group(3)  # 日期

    return year, month, day

# 傳入url發送GET請求，並有retry機制
def send_get_request(url):
    retries = 0
    while retries < 3:
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            res = requests.get(url)
            if res.status_code == 200:
                return res
            else:
                print("\033[1;31mRequest failed\033[0m")
                print(f"Status code {res.status_code}. Retrying...")
        except requests.exceptions.Timeout:
            print("\033[1;31mTimeout\033[0m")
        except requests.exceptions.TooManyRedirects:
            print("\033[1;31mTooManyRedirects\033[0m")

        retries += 1
        time.sleep(2)  # 等待2秒後重試

    raise Exception("Max retries exceeded. Failed to get a successful response.")

# 取得上市個股資料
def get_listed_stock_data(date_str):  # date_str = "YYYYMMDD"
    year, month, day = get_date_info(date_str)

    # 如果有檔案則取得資料，沒有檔案則發送API取得資料
    try:
        with open(
            f"./data/Listed_Stock/{year}/{month}/{date_str}_listed.json",
            "r",
            encoding="utf-16",
        ) as f:
            json_data = json.load(f)
        return json_data
    except FileNotFoundError:
        url = f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={date_str}&type=ALLBUT0999&response=json&_=1726121461234"
        res = send_get_request(url)
        soup = BeautifulSoup(res.content, "html.parser")
        json_data = json.loads(soup.text)

        # 判斷查詢日期大於今日
        if "大於今日" in json_data["stat"]:
            raise Exception("查詢日期大於今日，請重新查詢!")

        # 判斷目錄存不存在，不存在則建立
        create_dir(f"./data/Listed_Stock/{year}/{month}/{date_str}_listed_origin.json")

        # 寫入檔案
        with open(
            f"./data/Listed_Stock/{year}/{month}/{date_str}_listed_origin.json",
            "w",
            encoding="utf-16",
        ) as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)

        # 轉檔 (只取得上市個股資料)
        listed_arr = []
        if "tables" in json_data:
            for item in json_data["tables"]:
                if "title" in item:
                    if "每日收盤行情" in item["title"]:
                        for data in item["data"]:
                            # index 0 證券代號    "0050",
                            # index 1 證券名稱    "元大台灣50",
                            # index 2 成交股數    "16,337,565",
                            # index 3 成交筆數    "15,442",
                            # index 4 成交金額    "2,900,529,886",
                            # index 5 開盤價      "176.10",
                            # index 6 最高價      "178.65",
                            # index 7 最低價      "176.10",
                            # index 8 收盤價      "178.30",
                            # index 9 漲跌(+/-)   "+</p>",
                            # index 10 漲跌價差    "6.45",
                            # index 11 最後揭示買價 "178.20",
                            # index 12 最後揭示買量 "5",
                            # index 13 最後揭示賣價 "178.30",
                            # index 14 最後揭示賣量 "103",
                            # index 15 本益比
                            obj = {}
                            for idx, fields in enumerate(item["fields"]):
                                if fields in [
                                    "成交股數",
                                    "成交筆數",
                                    "成交金額",
                                    "開盤價",
                                    "最高價",
                                    "最低價",
                                    "收盤價",
                                    "漲跌價差",
                                    "最後揭示買價",
                                    "最後揭示買量",
                                    "最後揭示賣價",
                                    "最後揭示賣量",
                                    "本益比",
                                ]:
                                    obj[fields] = (
                                        float(data[idx].replace(",", ""))
                                        if data[idx] != "--"
                                        else None
                                    )
                                    continue
                                if fields == "漲跌(+/-)":
                                    obj[fields] = (
                                        data[idx].replace("</p>", "").replace("X", "")
                                    )
                                    continue
                                obj[fields] = data[idx]
                            obj["Change"] = float(
                                obj["漲跌(+/-)"] + str(obj["漲跌價差"])
                            )
                            listed_arr.append(obj)

        # 寫入轉檔後的檔案
        with open(
            f"./data/Listed_Stock/{year}/{month}/{date_str}_listed.json",
            "w",
            encoding="utf-16",
        ) as f:
            json.dump(listed_arr, f, ensure_ascii=False, indent=2)

        return listed_arr

# 取得上櫃個股資料
def get_otc_market_data(date_str):  # date_str = "YYYYMMDD"
    year, month, day = get_date_info(date_str)
    # 如果有檔案則取得資料，沒有檔案則發送API取得資料
    try:
        with open(
            f"./data/OTC_Market/{year}/{month}/{date_str}_otc.json",
            "r",
            encoding="utf-16",
        ) as f:
            json_data = json.load(f)
        return json_data
    except FileNotFoundError:
        # 如果查詢日期大於今日，則回傳錯誤
        input_date = datetime.strptime(date_str, "%Y%m%d")
        today = datetime.today()
        if input_date > today:
            raise Exception("查詢日期大於今日，請重新查詢!")

        url = f"https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes?date={year}%2F{str(month)}%2F{str(day)}&id=&response=json"
        res = send_get_request(url)
        soup = BeautifulSoup(res.content, "html.parser")
        json_data = json.loads(soup.text)

        # 櫃買的查詢日期如果大於今日，它會顯示最後一天有資料的日期。 如果是假日也會顯示有資料的日期。
        # 所以要先看它回傳的資料顯示日期是甚麼時候
        date_str_new = json_data["date"]
        year, month, day = get_date_info(date_str_new)

        # 判斷目錄存不存在，不存在則建立
        create_dir(f"./data/OTC_Market/{year}/{month}/{date_str_new}_otc_origin.json")

        # 寫入檔案
        with open(
            f"./data/OTC_Market/{year}/{month}/{date_str_new}_otc_origin.json",
            "w",
            encoding="utf-16",
        ) as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)

        # 轉檔 (只取得上櫃個股資料)
        otc_arr = []
        if "tables" in json_data:
            for item in json_data["tables"]:
                if "title" in item:
                    if "上櫃股票行情" in item["title"]:
                        for data in item["data"]:
                            obj = {}
                            for idx, fields in enumerate(item["fields"]):
                                if fields in [
                                    "收盤",
                                    "漲跌",
                                    "開盤",
                                    "最高",
                                    "最低",
                                    "均價",
                                    "成交股數",
                                    "成交金額",
                                    "成交金額(元)",
                                    "成交筆數",
                                    "最後買價",
                                    "最後買量(千股)",
                                    "最後賣價",
                                    "最後賣量(千股)",
                                    "發行股數",
                                    "次日 參考價",
                                    "次日 漲停價",
                                    "次日 跌停價",
                                ]:
                                    try:
                                        obj[fields] = float(data[idx].replace(",", ""))
                                    except ValueError:
                                        obj[fields] = None
                                    continue
                                obj[fields] = data[idx]
                            otc_arr.append(obj)

        # 寫入轉檔後的檔案
        with open(
            f"./data/OTC_Market/{year}/{month}/{date_str_new}_otc.json",
            "w",
            encoding="utf-16",
        ) as f:
            json.dump(otc_arr, f, ensure_ascii=False, indent=2)
        
        return otc_arr

# 與資料庫連線
def connet_to_db():
    with open(r"./dbconfig.json", "r") as f:
        mssql = json.load(f)
    server = mssql["server"]
    database = mssql["database"]
    username = mssql["username"]
    password = mssql["password"]
    cnxn = pyodbc.connect(
        "DRIVER={SQL Server};SERVER="
        + server
        + ";DATABASE="
        + database
        + ";UID="
        + username
        + ";PWD="
        + password
    )
    cursor = cnxn.cursor()

    return cnxn, cursor

# 關閉連接
def close_db_connection(cnxn, cursor):
    cursor.close()
    cnxn.close()

# 建立臨時表，並回傳插入臨時表的 SQL 語句
def create_stock_tmp_table(cursor):
    cursor.execute("drop table if exists #TempDayInfo")
    cursor.execute(
        """
    CREATE TABLE #TempDayInfo (
        Code NVARCHAR(50),
        [Name] NVARCHAR(255),
        [Date] DATE,
        [TradeVolume] BIGINT,  
        [TradeValue] BIGINT,  
        [OpeningPrice] FLOAT,
        [HighestPrice] FLOAT,
        [LowestPrice] FLOAT,
        ClosingPrice FLOAT,
        [Change] FLOAT,
        [Transaction] BIGINT,
        MonthlyAveragePrice FLOAT,
        PE FLOAT,
    )
    """
    )

    # 構建批量插入臨時表的 SQL 語句
    insert_temp_sql = """
    INSERT INTO #TempDayInfo (Code, [Name], [Date], [TradeVolume], [TradeValue], [OpeningPrice], [HighestPrice], [LowestPrice], [ClosingPrice], [Change], [Transaction], [MonthlyAveragePrice], [PE]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    return insert_temp_sql

# 將臨時表中的資料插入到目標表中，並檢查Code是否存在
def insert_stock_data(cnxn, cursor):
    merge_sql = """
    ;with Tmp as(
        select S.OID, T.Date, T.[TradeVolume], T.[TradeValue], T.[OpeningPrice], T.[HighestPrice], T.[LowestPrice], T.[ClosingPrice], T.[Change], T.[Transaction], T.MonthlyAveragePrice, T.PE
        from #TempDayInfo T, vd_Stock S
        where T.Code = S.Code
    )
    INSERT INTO DayInfo (OID, [Date], [TradeVolume], [TradeValue], [OpeningPrice], [HighestPrice], [LowestPrice], [ClosingPrice], [Change], [Transaction], [MonthlyAveragePrice], [PE])
    SELECT OID, [Date], [TradeVolume], [TradeValue], [OpeningPrice], [HighestPrice], [LowestPrice], [ClosingPrice], [Change], [Transaction], [MonthlyAveragePrice], [PE]
    FROM Tmp
    WHERE not exists(select * from DayInfo D where D.[Date] = Tmp.[Date] and D.OID = Tmp.OID);

    update DayInfo set PE = T.PE
    from #TempDayInfo T, vd_Stock S
    where DayInfo.[Date] = T.[Date] and DayInfo.OID = S.OID and S.Code = T.Code and DayInfo.PE is null and T.PE is not null;
    """

    cursor.execute(merge_sql)

    # 提交變更
    cnxn.commit()

# 插入不在資料庫的股票資料 market = 上市/上櫃
def insert_new_stock(cnxn, cursor, market):
    insert_sql = f"""
    INSERT INTO Object (Type, CName, CDes)
    SELECT 5, Code, Name
    FROM #TempDayInfo T
    WHERE not exists(select 1 from vd_Stock S where S.Code = T.Code);

    INSERT INTO Stock (SID, Market)
    SELECT (select OID from Object where CName = T.Code and Type = 5), '{market}'
    FROM #TempDayInfo T
    WHERE not exists(select 1 from vd_Stock S where S.Code = T.Code);
    """

    cursor.execute(insert_sql)

    # 提交變更
    cnxn.commit()

# 將上市資料寫入資料庫中
def insert_listed_stock_data(date_str):
    data = get_listed_stock_data(date_str)
    if data is None or len(data) == 0:
        return

    cnxn, cursor = connet_to_db()
    insert_tmp_sql = create_stock_tmp_table(cursor)

    data_to_insert = [
        (
            item["證券代號"],  # Code
            item["證券名稱"],  # Name
            date_str,  # Date
            item["成交股數"],  # TradeVolume 成交股數
            item["成交金額"],  # TradeValue 成交金額
            item["開盤價"],  # OpeningPrice 開盤價
            item["最高價"],  # HighestPrice 最高價
            item["最低價"],  # LowestPrice 最低價
            item["收盤價"],  # ClosingPrice 收盤價
            item["Change"],  # Change 漲跌價差
            item["成交筆數"],  # Transaction 成交筆數
            None,  # MonthlyAveragePrice 月均價
            item["本益比"],  # PE 本益比
        )
        for item in data
    ]

    cursor.executemany(insert_tmp_sql, data_to_insert)

    # 插入不在資料庫的股票資料 (先執行，避免沒有插入當天資料)
    insert_new_stock(cnxn, cursor, "上市")

    # 將臨時表中的資料插入到目標表中，並檢查Code是否存在
    insert_stock_data(cnxn, cursor)

    # 關閉 DataBase 連線
    close_db_connection(cnxn, cursor)

# 將上櫃資料寫入資料庫中
def insert_otc_market_data(date_str):
    data = get_otc_market_data(date_str)
    if data is None or len(data) == 0:
        return

    cnxn, cursor = connet_to_db()
    insert_tmp_sql = create_stock_tmp_table(cursor)

    data_to_insert = [
        (
            item["代號"],  # Code
            item["名稱"],  # Name
            date_str,  # Date
            item["成交股數"],  # TradeVolume 成交股數
            item["成交金額(元)"],  # TradeValue 成交金額
            item["開盤"],  # OpeningPrice 開盤價
            item["最高"],  # HighestPrice 最高價
            item["最低"],  # LowestPrice 最低價
            item["收盤"],  # ClosingPrice 收盤價
            item["漲跌"],  # Change 漲跌價差
            item["成交筆數"],  # Transaction 成交筆數
            None,  # MonthlyAveragePrice 月均價
            None,  # PE 本益比
        )
        for item in data
    ]

    cursor.executemany(insert_tmp_sql, data_to_insert)

    # 插入不在資料庫的股票資料 (先執行，避免沒有插入當天資料)
    insert_new_stock(cnxn, cursor, "上櫃")

    # 將臨時表中的資料插入到目標表中，並檢查Code是否存在
    insert_stock_data(cnxn, cursor)

    # 關閉 DataBase 連線
    close_db_connection(cnxn, cursor)

# 將上市/上櫃股票資料寫入資料庫中
def insert_stock_to_db(date_str):
    insert_listed_stock_data(date_str)
    insert_otc_market_data(date_str)