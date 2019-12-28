### 交易監控機 ATMMonitor

群益 API 官網
<https://www.capital.com.tw/Service2/download/API.asp>

### 提供功能:
1. 透過sp檢測Tick完整性, 是否delay過久
2. 檢測SKOrder是否正常啟用
3. 重新發送未送出的line message

### 使用方法
1. 填入config裡的參數
2. 在工作排程設定固定時間啟用, 個人是設定8:46 and 15:01啟動, 每分鐘執行一次



