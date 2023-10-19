from typing import List, Tuple, Type


def display_query_result_line_by_line(
    query_result: List[Tuple[Type]]
):
    for row in query_result:
        print(row)
