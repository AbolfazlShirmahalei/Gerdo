from config import TRX_PATH
from question1.queries import QUESTION1_PART_A_QUERY_PATH
from utils import (
    Database,
    read_query,
    display_query_result_line_by_line,
)


def question1(
    database: Database,
    part_a_query: str,
):
    part_a_query_result = database.query(part_a_query)
    print("Question1 - Part a")
    print("Daily Number of Transactions:")
    display_query_result_line_by_line(
        query_result=part_a_query_result,
    )


if __name__ == "__main__":
    database = Database(
        data_name_to_address={
            "TRX": TRX_PATH,
        },
    )

    part_a_query = read_query(query_path=QUESTION1_PART_A_QUERY_PATH)

    question1(
        database=database,
        part_a_query=part_a_query,
    )
