'''
每天早上爬取前一天的股票數據 (利用 windows 排程器)
'''

from stock_fuction import *
from datetime import datetime, timedelta

def get_custom_date():
    now = datetime.now()

    # 獲取當前的星期幾和時間（24小時制）
    weekday = now.weekday()  # 星期一是0，星期日是6
    hour = now.hour

    # 定義星期五的日期
    last_friday = now - timedelta(days=(weekday - 4 if weekday >= 5 else 7 + weekday - 4))

    # 如果是星期六8點後到星期二8點前，返回星期五的日期
    if (weekday == 5 and hour >= 8) or (weekday in [6, 0] or (weekday == 1 and hour < 8)):
        custom_date = last_friday
    elif hour >= 8:
        # 其他日子8點後，返回昨天的日期
        custom_date = now - timedelta(days=1)
    else:
        # 其他日子8點前，返回前天的日期
        custom_date = now - timedelta(days=2)

    return custom_date.strftime('%Y%m%d')

if __name__ == '__main__':
    try:
        # 獲取日期
        date_str = get_custom_date()
        
        # 爬取股票數據進資料庫
        insert_stock_to_db(date_str)

        write_log(f'爬取股票資料成功！日期：{date_str}')
    except Exception as e:
        print(f'Error: {e}')
        write_log(f'Error: {e}')