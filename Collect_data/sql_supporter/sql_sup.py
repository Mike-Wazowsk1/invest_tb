from pyspark import SQLContext
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import keyring
from tqdm.auto import tqdm
from sklearn.utils import parallel_backend
from joblibspark import register_spark
from joblib import Parallel,delayed
from joblibspark import register_spark
import re

class Sqler:
    def __init__(self, url="jdbc:postgresql://localhost:5432/shares", user=None, password=None):
        self.spark = SparkSession.builder.config("spark.jars", "postgresql-42.3.5.jar") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.3.5") \
            .config('spark.driver.maxResultSize', '20g').config("spark.executor.memory", "20g") \
            .config("spark.driver.memory", "20g").config("spark.executor.memory", "20g") \
            .config("spark.shuffle.file.buffer", '40').config("spark.scheduler.listenerbus.eventqueue.capacity",
                                                              "10000000").config("driver", "org.postgresql.Driver") \
            .master("local").appName("tinkof-invest").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.spark.conf.set("spark.sql.execution.arrow.enabled", 'True')
        self._url = url
        self._user = user
        self._pass = password

    def read_sql(self, q):
        return self.spark.read.format("jdbc") \
            .option("url", self._url) \
            .option('query', q) \
            .option('user', self._user) \
            .option("password", self._pass) \
            .option("driver", "org.postgresql.Driver") \
            .load()

    def select_agg(self, agg, col, figi, table):
        q = f"""
                SELECT {agg}({col}) from {table}
                where figi = '{figi}'

            """
        return self.read_sql(q)

    def get_by_condition(self, condition, value, col, table):
        q = f"""
            SELECT {col} from {table}
            where {condition} = '{value}'
        """
        return self.read_sql(q)

    def select_agg_with_date_figi(self, agg, col, figi, from_, to, table):
        if type(from_) == (tuple or list):
            from_ = datetime(*from_)
        elif type(from_) == datetime:
            from_ = from_
        if type(to) == str and to.lower() == 'now':
            to = datetime.now()
        elif type(to) == tuple:
            to = datetime(*to)
        elif type(to) == str and 'd' in to:
            to = from_ + timedelta(days=int(to[:-1]))
        q = f"""
                SELECT {agg}({col}) from {table}
                where figi = '{figi}'
                and time::timestamp::date  between to_date('{from_}','YYYY-mm-dd') and to_date('{to}','YYYY-mm-dd')

            """
        return self.read_sql(q)

    def select_agg_with_date_all_figi(self, agg, col, from_, to, table):
        q = f"""
                SELECT figi,{agg}({col}) from {table}
                where time::timestamp::date  between to_date('{from_}','YYYY-mm-dd') and to_date('{to}','YYYY-mm-dd')
                group by figi

            """
        return self.read_sql(q)

    def get_last_date(self,table):
        q = f"""
                 select min(time) from (select max(time) as time 
                     from {table} 
                     group by figi) as foo 
                 """
        return self.read_sql(q)

    def get_last_date_by_figi(self, figi,table):
        q = f"""
            SELECT MAX(time) from {table}
            where figi = '{figi}'
            """
        return self.read_sql(q)

    def insert(self, df, table):
        sqlcontext = SQLContext(self.spark)
        df = sqlcontext.createDataFrame(df)

        df.write.format('jdbc').options(url=self._url,driver="org.postgresql.Driver",dbtable=f'{table}',user=self._user,password=self._pass).mode('append').save()

    def create_table(self, df, table):
        mode = 'append'
        url = self._url
        properties = {"user": self._user, "password": self._pass,"driver":"org.postgresql.Driver"}
        spark_df = self.spark.createDataFrame(df)
        spark_df.write.jdbc(url=url, table=f"{table}", mode=mode, properties=properties)

    def get_ma(self,figi,col,window,table):

        q = f"""
   
        select avg(c.{col})
            over(order by c.time rows between {window-1} preceding and current row) as avg_price 
                from {table} c
                where figi = '{figi}'
            order by time desc
            limit 1
            """
        return self.read_sql(q).collect()

# USER = 'nikolay'
# SQL_PASS = keyring.get_password('SQL', USER)
# sql = Sqler(user=USER,password=SQL_PASS)
# figi = sql.read_sql("""select distinct figi from shares_info where currency= 'rub'""").collect()
# #sql.get_ma()
# res = Parallel(n_jobs=-1,prefer='threads')(delayed(sql.get_ma)(figi=f.figi,col='close',window=8,table='candles_hour_rus') for f in tqdm(figi))

