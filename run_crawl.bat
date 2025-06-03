@echo off
chcp 65001
REM Kích hoạt môi trường conda và chạy lệnh trong cùng một dòng

call C:\Users\ADMIN\anaconda3\Scripts\activate.bat bigdata-env

cd /d D:\bigdata_test\crawl_dantri

python main.py sync --num_links 1000 --max_pagination 10 --force_crawl

pause
