ydb -e grpcs://ydb.serverless.yandexcloud.net:2135 --yc-token-file ~/.tokens/.ya_token -d /ru-central1/b1gmu1d5ui51bunc0i7q/etnuonscj6nljs8c0rdc \
import file csv \
-p transactions_v2 \
~/Documents/Edu/etl/exam_m4/data/transactions_v2.csv \
--delimiter , \
--header