#### 計算 PDCare 裝置設定門檻值計算工具

- 執行 pdstat_gui 直接將 PDSimply 產出的 excel 拖入即可以得到統計值
- 如果需要自訂統計比例與發生次數, 調整 config.example.yml 內的值代表 std ratio
  完成後改名為 config.yml 即會取代程式內的預設比例

- update@2025-03-21
  - python 版本調整為 3.11
  - sqlalchemy 保留在較舊版本避免語法不支援
  - 在完成統計計算後，將 load_trend 的字典清空避免無用數據佔用大量記憶體
