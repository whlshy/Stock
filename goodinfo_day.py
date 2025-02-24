from stock_fuction import *
from datetime import datetime, timedelta
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

def get_goodinfo_data(date_str = ""): # 2025-02-05
	datas = []

	try:
		with open(f'./data/goodinfo/{date_str}_10and20日正要黃金交叉向上.json', 'r', encoding='utf-8') as f:
			json_data = json.load(f)
		return json_data
	except FileNotFoundError:
		# 目標 URL
		url = "https://goodinfo.tw/tw2/StockList.asp?MARKET_CAT=%E8%87%AA%E8%A8%82%E7%AF%A9%E9%81%B8&INDUSTRY_CAT=%E6%88%91%E7%9A%84%E6%A2%9D%E4%BB%B6&FL_ITEM0=%E8%82%A1%E6%9C%AC+%28%E5%84%84%E5%85%83%29&FL_VAL_S0=0&FL_VAL_E0=&FL_ITEM1=%E5%9D%87%E7%B7%9A%E6%BC%B2%E8%B7%8C%28%E5%85%83%29%E2%80%9320%E6%97%A5&FL_VAL_S1=0&FL_VAL_E1=&FL_ITEM2=%E5%9D%87%E7%B7%9A%E6%BC%B2%E8%B7%8C%28%E5%85%83%29%E2%80%9360%E6%97%A5&FL_VAL_S2=0&FL_VAL_E2=&FL_ITEM3=%E6%97%A5%E5%9D%87%E6%88%90%E4%BA%A4%E5%BC%B5%E6%95%B8%28%E5%BC%B5%29%E2%80%93%E8%BF%915%E6%97%A5&FL_VAL_S3=500&FL_VAL_E3=&FL_ITEM4=&FL_VAL_S4=&FL_VAL_E4=&FL_ITEM5=&FL_VAL_S5=&FL_VAL_E5=&FL_ITEM6=&FL_VAL_S6=&FL_VAL_E6=&FL_ITEM7=&FL_VAL_S7=&FL_VAL_E7=&FL_ITEM8=&FL_VAL_S8=&FL_VAL_E8=&FL_ITEM9=&FL_VAL_S9=&FL_VAL_E9=&FL_ITEM10=&FL_VAL_S10=&FL_VAL_E10=&FL_ITEM11=&FL_VAL_S11=&FL_VAL_E11=&FL_RULE0=%E5%9D%87%E7%B7%9A%E4%BD%8D%E7%BD%AE%7C%7C%E5%9D%87%E5%83%B9%E7%B7%9A%E5%8D%B3%E5%B0%87%E4%BA%A4%E5%8F%89%E5%90%91%E4%B8%8A+%2820%E6%97%A5%2F60%E6%97%A5%29%40%40%E5%9D%87%E5%83%B9%E7%B7%9A%E5%8D%B3%E5%B0%87%E4%BA%A4%E5%8F%89%E5%90%91%E4%B8%8A%40%4020%E6%97%A5%2F60%E6%97%A5&FL_RULE1=&FL_RULE2=&FL_RULE3=&FL_RULE4=&FL_RULE5=&FL_RANK0=&FL_RANK1=&FL_RANK2=&FL_RANK3=&FL_RANK4=&FL_RANK5=&FL_FD0=%E6%88%90%E4%BA%A4%E5%83%B9+%28%E5%85%83%29%7C%7C1%7C%7C0%7C%7C%3E%7C%7C%E5%9D%87%E7%B7%9A%E4%BD%8D%E7%BD%AE%28%E5%85%83%29%E2%80%9320%E6%97%A5%7C%7C1%7C%7C0&FL_FD1=%E5%9D%87%E7%B7%9A%E4%BD%8D%E7%BD%AE%28%E5%85%83%29%E2%80%9320%E6%97%A5%7C%7C1%7C%7C0%7C%7C%3C%7C%7C%E5%9D%87%E7%B7%9A%E4%BD%8D%E7%BD%AE%28%E5%85%83%29%E2%80%9360%E6%97%A5%7C%7C1%7C%7C0&FL_FD2=&FL_FD3=&FL_FD4=&FL_FD5=&FL_SHEET=%E7%A7%BB%E5%8B%95%E5%9D%87%E7%B7%9A&FL_SHEET2=%E7%9B%AE%E5%89%8D%E4%BD%8D%E7%BD%AE1%28%E5%85%83%29&FL_MARKET=%E4%B8%8A%E5%B8%82%2F%E4%B8%8A%E6%AB%83&FL_QRY=%E6%9F%A5++%E8%A9%A2"

		# 初始化 Selenium WebDriver
		options = webdriver.ChromeOptions()
		options.add_argument("--headless")  # 無頭模式，不開啟瀏覽器視窗
		options.add_argument("--disable-gpu")  # 禁用 GPU 加速
		options.add_argument("--no-sandbox")  # 禁用沙盒模式
		options.add_argument("--disable-dev-shm-usage")  # 避免內存不足問題

		# 使用 webdriver-manager 自動下載 ChromeDriver
		driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

		try:
			driver.get(url)
			# 明確等待表格加載完成
			WebDriverWait(driver, 15).until(
				EC.presence_of_element_located((By.ID, "tblStockList"))
			)
			
			# 解析 HTML
			soup = BeautifulSoup(driver.page_source, 'html.parser')
			table = soup.find('table', {'id': 'tblStockList'})
			
			if not table:
				print("找不到表格")
				exit()

			# 提取表頭
			headers = []
			header_row = table.find('tr', class_='bg_h2')
			for th in header_row.find_all('th'):
				header_text = th.get_text(strip=True)
				headers.append(header_text)
			
			# 調整表頭名稱 (根據你的需求)
			headers = ["代號", "名稱", "成交", "漲跌價", "漲跌幅", "成交張數", "更新日期",
					"5日均線", "10日均線", "15日均線", "20日均線", "50日均線", "60日均線",
					"100日均線", "120日均線", "200日均線", "240日均線"]
			
			# 提取數據行
			data_rows = table.find_all('tr', {'id': lambda x: x and x.startswith('row')})
			date_str = None

			for index, row in enumerate(data_rows):
				cells = row.find_all(['th', 'td'])
				data_obj = {}
				
				for idx, cell in enumerate(cells):
					# 提取主要文本
					text = cell.get_text(strip=True)
					
					# 特殊處理均線數據 (包含趨勢符號 ↗↘)
					if 'title' in cell.attrs:
						title = cell['title']
						# 合併文本和 title 數據 (依需求調整)  f"{text} | {title.split('日期範圍')[0].strip()}"
						text = f"{text}"

					if(headers[idx] == "更新日期" and index == 0):
						current_year = datetime.now().year
						date_str = f"{current_year}-{text.split('/')[0].zfill(2)}-{text.split('/')[1].zfill(2)}"

					data_obj[headers[idx]] = text

					if(headers[idx] == "更新日期"):
						data_obj[headers[idx]] = date_str

				datas.append(data_obj)

			if(date_str is not None):
				create_dir(f'./data/goodinfo/{date_str}_10and20日正要黃金交叉向上.json')
				with open(f'./data/goodinfo/{date_str}_10and20日正要黃金交叉向上.json', 'w', newline='', encoding='utf-8') as f:
					json.dump(datas, f, ensure_ascii=False, indent=2)
		finally:
			driver.quit()
			return datas

def insert_goodinfo_data_to_db():
    datas = get_goodinfo_data()

    if(datas is None or len(datas) == 0):
        return
    else:
        date_str = datas[0]["更新日期"]

    cnxn, cursor = connet_to_db()

    # 將goodinfo結果寫入資料庫
    data_to_insert = [
        (
            item["代號"],
            item["名稱"],
            item["成交"],
            item["漲跌價"],
            item["漲跌幅"],
            item["成交張數"],
            item["更新日期"],
            item["5日均線"],
            item["10日均線"],
            item["15日均線"],
            item["20日均線"],
            item["50日均線"],
            item["60日均線"],
            item["100日均線"],
            item["120日均線"],
            item["200日均線"],
            item["240日均線"]
        )
        for item in datas
    ]

    cursor.execute("drop table if exists #TempDayInfo")
    cursor.execute(
        """
    CREATE TABLE #TempDayInfo (
        StockId nvarchar(10),
        StockName nvarchar(50),
        DealPrice nvarchar(50),
        ChangePrice nvarchar(50),
        ChangePercent nvarchar(50),
        DealAmount nvarchar(50),
        UpdateDate date,
        MA5 nvarchar(50),
        MA10 nvarchar(50),
        MA15 nvarchar(50),
        MA20 nvarchar(50),
        MA50 nvarchar(50),
        MA60 nvarchar(50),
        MA100 nvarchar(50),
        MA120 nvarchar(50),
        MA200 nvarchar(50),
        MA240 nvarchar(50)
    )
    """
    )

    # 構建批量插入臨時表的 SQL 語句
    insert_temp_sql = """
    INSERT INTO #TempDayInfo 
    (StockId, StockName, DealPrice, ChangePrice, ChangePercent, DealAmount, UpdateDate, MA5, MA10, MA15, MA20, MA50, MA60, MA100, MA120, MA200, MA240) 
    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    cursor.executemany(insert_temp_sql, data_to_insert)
        
    insert_class_sql = f"""
    if(not exists(select 1 from vs_SubClass where CID = 6 and CName = '{date_str}'))
        exec xp_insertClass 6, 2, '{date_str}', '', '', '', 52091, 1
    """

    cursor.execute(insert_class_sql)

    cnxn.commit()

    insert_co_sql = f"""
    insert into CO(CID, OID, Des)
    select (select CCID from vs_SubClass where CID = 6 and CName = '{date_str}'), S.OID, '{date_str}'
    from #TempDayInfo T, vd_Stock S
    where T.StockId = S.Code and not exists(select 1 from CO where CID = (select CCID from vs_SubClass where CID = 6 and CName = '{date_str}') and OID = S.OID)
    """

    cursor.execute(insert_co_sql)

    cnxn.commit()

    close_db_connection(cnxn, cursor)

# 最好每天下午四點後再執行 這樣才是當天行情
if __name__ == "__main__":
	insert_goodinfo_data_to_db()