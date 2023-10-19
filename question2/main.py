from config import BUG_PATH
from question2.queries import QUESTION2_QUERY_PATH
from utils import (
    Database,
    read_query,
    display_query_result_line_by_line,
)


def question1(
    database: Database,
    query: str,
):
    query_result = database.query(query)
    print("Question2:")
    display_query_result_line_by_line(
        query_result=query_result,
    )


if __name__ == "__main__":
    database = Database(
        data_name_to_address={"BUG": BUG_PATH},
    )

    query = read_query(query_path=QUESTION2_QUERY_PATH)

    question1(
        database=database,
        query=query,
    )
