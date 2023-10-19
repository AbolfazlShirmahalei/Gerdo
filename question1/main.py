from config import TRX_PATH, LOGIN_PATH, CUSTOMERS_PATH
from question1.queries import (
    QUESTION1_PART_A_QUERY_PATH,
    QUESTION1_PART_B_QUERY_PATH,
    QUESTION1_PART_C_QUERY_PATH,
    QUESTION1_PART_D_QUERY_PATH,
)
from utils import (
    Database,
    read_query,
    display_query_result_line_by_line,
)


def question1(
    database: Database,
    part_a_query: str,
    part_b_query: str,
    part_c_query: str,
    part_d_query: str,
):
    part_a_query_result = database.query(part_a_query)
    print("Question1 - Part a")
    print("Daily Number of Transactions:")
    display_query_result_line_by_line(
        query_result=part_a_query_result,
    )

    part_b_query_result = database.query(part_b_query)
    print("\nQuestion1 - Part b")
    print("3 Customers Who Visited the Application the Most:")
    display_query_result_line_by_line(
        query_result=part_b_query_result,
    )

    part_c_query_result = database.query(part_c_query)
    print("\nQuestion1 - Part c:")
    display_query_result_line_by_line(
        query_result=part_c_query_result,
    )

    part_d_query_result = database.query(part_d_query)
    print("\nQuestion1 - Part d")
    print(
        "Customers whose total value of transactions exceeds "
        "1 million IRT during any 3 consecutive days:"
    )
    display_query_result_line_by_line(
        query_result=part_d_query_result,
    )


if __name__ == "__main__":
    database = Database(
        data_name_to_address={
            "TRX": TRX_PATH,
            "LOGIN": LOGIN_PATH,
            "CUSTOMERS": CUSTOMERS_PATH,
        },
    )

    part_a_query = read_query(query_path=QUESTION1_PART_A_QUERY_PATH)
    part_b_query = read_query(query_path=QUESTION1_PART_B_QUERY_PATH)
    part_c_query = read_query(query_path=QUESTION1_PART_C_QUERY_PATH)
    part_d_query = read_query(query_path=QUESTION1_PART_D_QUERY_PATH)

    question1(
        database=database,
        part_a_query=part_a_query,
        part_b_query=part_b_query,
        part_c_query=part_c_query,
        part_d_query=part_d_query,
    )
