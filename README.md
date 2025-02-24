請先建立 `dbconfig.json` 在根目錄中

dbconfig.json：
```json
{
  "server": ".",
  "database": "I3S_Stock",
  "username": "username",
  "password": "password"
}
```

## 打包

```sh
pyinstaller --onefile stock_day_all.py
```
