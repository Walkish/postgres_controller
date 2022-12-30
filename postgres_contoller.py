import pandas as pd
import psycopg2
import logging
import json
from psycopg2.extras import execute_values
from typing import List, Union, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="log.txt", format="%(asctime)s - %(message)s", datefmt="%d-%b-%y %H:%M:%S"
)


class PostgresController:
    def __init__(
        self,
        host: Union[str, int],
        port: int,
        user: str,
        password: str,
        database: str,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.is_reconnect = False
        self.reconnect_number = 0

    def _connect(self):
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )

    def __del__(self):
        if not self._connection_is_closed():
            self.conn.close()
    def _connection_is_closed(self) -> bool:
        return self.conn is None or self.conn.closed != 0

    def _check_and_reconnect(self):
        if self._connection_is_closed():
            if self.is_reconnect:
                self.reconnect_number += 1
            else:
                self.is_reconnect = True
            self._connect()

    def select(self, query: str) -> List[List]:
        rows, _ = self.select_with_columns(query)
        return rows

    def select_with_columns(self, query) -> Tuple[List[List], List[str]]:
        self._check_and_reconnect()
        with self.conn as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
            rows = [list(row) for row in rows]
            return rows, columns

    def execute(self, query: str):
        self._check_and_reconnect()
        with self.conn as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)

    def _split_to_rows_columns(self, df: pd.DataFrame):
        return list(df.columns), [list(row) for row in df.values]

    def cast_dict_to_json(self, dictionary):
        """
        The method casts the given pd.Dataframe column
        which contains a nested structure into a JSON object.

        example:

        data = {
            'userId': 31788,
            'platform': 'APP_ANDROID',
            'durationMs': 3249,
            'position': 18,
            'timestamp': 1568308979000,
            'owners': {
                    'user': [11540]
                    },
            'resources': {
                    'USER_PHOTO': [6121]
                    }
                }

        df = pd.DataFrame(data)


        self.insert_dataframe(df, "table_name")  --- > WRONG.
        It will return the error :
        psycopg2.ProgrammingError: can't adapt type 'dict'

        CORRECT:

        df["owners"] = df.owners.map(pg.cast_dict_to_json)
        df["resources"] = df.resources.map(pg.cast_dict_to_json)
        self.insert_dataframe(df, "table_name")
        """
        return json.dumps(dictionary, ensure_ascii=False)

    def insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        table_schema: str = "public",
    ):
        self._check_and_reconnect()
        columns, rows = self._split_to_rows_columns(df)
        start = datetime.now()

        with self.conn as conn:
            with conn.cursor() as cursor:
                query = f"INSERT INTO {table_schema}.{table_name} ({', '.join(columns)}) VALUES %s"
                execute_values(cursor, query, rows, page_size=50000)
                duration = datetime.now() - start
                newline = "\n"
                logger.warning(
                    f"Data inserted in table: '{table_name.upper()}'"
                    f"{newline}{len(rows):_d} rows inserted"
                    f"{newline}Time: {duration.seconds} seconds"
                )

                total_duration = datetime.now() - start
                logger.warning(
                    f"Pandas job is finished. Total time: {total_duration.seconds} seconds"
                )
