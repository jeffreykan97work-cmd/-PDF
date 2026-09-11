# SMG Monthly PDF

## 跑 PDF 嘅連結

https://github.com/jeffreykan97work-cmd/-PDF/actions/workflows/pdf.yml

1. 開住個連結
2. 點 **Run workflow**
3. 可以填年份、月份；留空 = 上個月
4. 跑完之後入該次 Run，下載 **SMG-Monthly-Report** artifact

## 點樣邀請同事（只跑唔改 code）

GitHub 要 **Write** 先可以點 Run workflow，所以邀請時請選 Write。  
但 `main` 已加 ruleset：

- 唔可以直接 push 去 `main`
- 唔可以 force push / 刪 `main`
- 改程式一定要開 Pull Request，並要 owner 批核

邀請路徑：Repo → **Settings** → **Collaborators** → Add people → Role 選 **Write**

邀完之後將上面個 Actions 連結傳俾佢。

## 注意

- **Build Windows EXE Package** 只有倉庫 owner 可以跑
- PDF workflow 只讀取程式，唔會改 repo 內容
