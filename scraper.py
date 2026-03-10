name: SMG Monthly PDF Report

on:
  # 每月1號早上9點自動執行（UTC+8 即01:00 UTC）
  schedule:
    - cron: '0 1 1 * *'
  # 亦可手動觸發（支援指定年月）
  workflow_dispatch:
    inputs:
      year:
        description: '年份（留空=自動上個月）'
        required: false
        default: ''
      month:
        description: '月份（留空=自動上個月）'
        required: false
        default: ''

jobs:
  generate-report:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Cache pip packages
        uses: actions/cache@v4
        with:
          path: ~/.cache/pip
          key: ${{ runner.os }}-pip-${{ hashFiles('requirements.txt') }}
          restore-keys: |
            ${{ runner.os }}-pip-

      - name: Install Python dependencies
        run: pip install -r requirements.txt

      - name: Install Playwright Chromium
        run: playwright install chromium --with-deps

      - name: Run scraper
        run: |
          YEAR="${{ github.event.inputs.year }}"
          MONTH="${{ github.event.inputs.month }}"
          ARGS=""
          [ -n "$YEAR"  ] && ARGS="$ARGS --year $YEAR"
          [ -n "$MONTH" ] && ARGS="$ARGS --month $MONTH"
          python smg_monthly_scraper.py $ARGS

      - name: Upload PDF report as artifact
        uses: actions/upload-artifact@v4
        with:
          name: SMG-Monthly-Report
          path: SMG_Monthly_Report_*.pdf
          retention-days: 90

      - name: Upload individual PDFs (tmp folder)
        uses: actions/upload-artifact@v4
        with:
          name: SMG-Individual-PDFs
          path: smg_tmp_*/
          retention-days: 30
          if-no-files-found: warn
