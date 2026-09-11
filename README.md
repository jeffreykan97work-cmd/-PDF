# SMG Monthly PDF

## 同事一按鍵跑（唔使登入）

頁面：https://jeffreykan97work-cmd.github.io/-PDF/

傳俾同事時要帶 `#t=你的token`，例如：

`https://jeffreykan97work-cmd.github.io/-PDF/#t=你的token`

同事開條連結 → 可填年/月或留空 → 點「幫我出 PDF」 → 等完左去 Actions 下載 **SMG-Monthly-Report**。

你只要做一次 token：

1. 開 https://github.com/settings/personal-access-tokens
2. Generate new token → Fine-grained
3. Repository access 只選 `-PDF`
4. Permissions：**Actions = Read and write**，其余留 No access / Read
5. 生成後貼到條連結 `#t=` 後面

這個 token 只用來跑 workflow，唔會改程式。`main` 仍然鎖死。

## 你自己登入跑

https://github.com/jeffreykan97work-cmd/-PDF/actions/workflows/pdf.yml

1. 開住個連結
2. 點 **Run workflow**

3. 可以填年份、月份；留空 = 上個月

4. 跑完之後入該次 Run，下載 **SMG-Monthly-Report** artifact
