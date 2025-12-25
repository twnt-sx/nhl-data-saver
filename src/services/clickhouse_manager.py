import logging
from itertools import islice

import clickhouse_connect
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class ClickhouseManager:
    def __init__(self, *, host: str, port: int, username: str, password: str, database: str = 'default'):
        self.ch_client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database
        )

    def execute(self, sql_statement: str):
        logger.info(f'⏳ Выполнение запроса {sql_statement}')
        return self.ch_client.command(sql_statement)

    def count_column(self, table_name: str, column_name: str = '*', distinct: bool = False):
        sql_statement = f'''
            SELECT COUNT ({'DISTINCT ' if distinct else ''}{column_name})
            FROM {self.ch_client.database}.{table_name}
        '''
        return self.execute(sql_statement)

    def table_exists(self, table_name: str) -> bool:
        sql_statement = f'''
            EXISTS TABLE {self.ch_client.database}.{table_name}
        '''
        result = self.execute(sql_statement)
        return bool(result)

    def truncate_table(self, table_name: str, on_cluster: bool = True):
        sql_statement = f'''
            TRUNCATE TABLE {self.ch_client.database}.{table_name} {"ON CLUSTER '{cluster}' SYNC" if on_cluster else ''}
        '''
        self.execute(sql_statement)

    def drop_table(self, table_name: str, on_cluster: bool = True):
        sql_statement = f'''
            DROP TABLE {self.ch_client.database}.{table_name} {"ON CLUSTER '{cluster}' SYNC" if on_cluster else ''}
        '''
        self.execute(sql_statement)

    def query_pandas_df(self, sql_statement: str):
        logger.info(f'⏳ Выполнение запроса {sql_statement}')
        return self.ch_client.query_df(sql_statement)

    def query_dataset(self, sql_statement: str):
        logger.info(f'⏳ Выполнение запроса {sql_statement}')
        return self.ch_client.query(sql_statement)

    def query_game_ids(self, table_name: str, where: str = ''):
        """
        Возвращает датасет с уникальными идентификаторами игр, сохраненных в таблицу table_name.
        Используется, чтобы повторно не записывать в таблицу данные по уже сохраненным играм
        """

        sql_statement = f'''
            SELECT DISTINCT game_id
            FROM {self.ch_client.database}.{table_name}
            {where}
        '''
        return self.query_dataset(sql_statement)

    def insert_batches(self, df: DataFrame, table_name: str, batch_size: int = 1000):
        """
        Батчами по batch-size записывает полученный спарк-датафрейм в таблицу table
        """
        columns = df.columns
        it = df.toLocalIterator()

        while True:
            data = tuple(tuple(row[c] for c in columns) for row in islice(it, batch_size))

            if not data:
                break

            res = self.ch_client.insert(
                table=table_name,
                data=data,
                column_names=columns,
                database=self.ch_client.database
            )
            logger.info(f'#️⃣ Строк добавлено в {table_name} в рамках итерации: {res.summary.get("read_rows", 0)}')

    def close(self):
        self.ch_client.close()
