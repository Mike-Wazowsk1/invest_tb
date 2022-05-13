from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import keyring


class Sqler:
    def __init__(self, url="jdbc:postgresql://localhost:5432/shares", user=None, password=None):
        self.spark = SparkSession.builder.config("spark.jars", "postgresql-42.2.5.jar") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.2.5") \
            .config('spark.driver.maxResultSize', '20g').config("spark.executor.memory", "20g") \
            .config("spark.driver.memory", "20g").config("spark.executor.memory", "20g") \
            .config("spark.shuffle.file.buffer", '40').config("spark.scheduler.listenerbus.eventqueue.capacity",
                                                              "10000000") \
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
                SELECT figi,{agg}({col}) from {table}
                where time::timestamp::date  between to_date('{from_}','YYYY-mm-dd') and to_date('{to}','YYYY-mm-dd')
                group by figi

            """
        return self.read_sql(q)

    def get_last_date(self):
        q = """
            SELECT MAX(time) from candles_day
            """
        return self.read_sql(q)

    def get_last_date_by_figi(self, figi):
        q = f"""
            SELECT MAX(time) from candles_day
            where figi = '{figi}'
            """
        return self.read_sql(q)

    def insert(self, df, table):
        df = self.spark.createDataFrame(df)
        df.write.insertInto(tableName=table, overwrite=False)

    def create_table(self, df, table):
        mode = 'overwrite'
        url = self._url
        properties = {"user": self._user, "password": self._pass, "driver": "org.postgresql.Driver"}
        spark_df = self.spark.createDataFrame(df)
        spark_df.write.jdbc(url=url, table=f"{table}", mode=mode, properties=properties)
