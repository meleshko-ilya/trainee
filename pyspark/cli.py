import argparse


QUERY_HELP = """
1 - Number of movies per category
2 - Top 10 actors by rentals
3 - Category with max revenue
4 - Movies not in inventory
5 - Top 3 actors in Children category
6 - Cities with active/inactive customers
7 - Category with max rental hours for cities starting with 'a' or containing '-'
"""


def parse_args():
    parser = argparse.ArgumentParser(
        description="PySpark app",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("-q", type=int, required=True, help=QUERY_HELP)
    parser.add_argument("--out", default="output", help="Output directory")
    return parser.parse_args()
