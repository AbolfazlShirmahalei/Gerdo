import sqlite3
from typing import Dict, List, Type, Tuple

import pandas as pd

from utils.database.config import DATABASE_NAME


class Database:
    def __init__(
        self,
        data_name_to_address: Dict[str, str],
        data_base_name: str = DATABASE_NAME,
    ):
        connection = sqlite3.connect(data_base_name)

        for table_name, data_address in data_name_to_address.items():
            data = pd.read_csv(data_address)
            data.to_sql(table_name, con=connection, if_exists="replace")

        self.cursor = connection.cursor()

    def query(self, my_query: str) -> List[Tuple[Type]]:
        return self.cursor.execute(my_query, ).fetchall()
