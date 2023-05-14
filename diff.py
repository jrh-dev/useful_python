import re
from pyspark.sql import DataFrame


class Diff:
    """Class to find the difference between two dataframes"""

    def ___init__(self, df1, df2, code):
        self.df1 = df1
        self.df2 = df2
        self.check_is_spark_dataframe(df1, df2)
        self.columns = self.df1.columns + self.df2.columns
        self.req_cols = self.find_columns(self.columns, code)

    # code to check df1 & df2 are spark dataframes

    def diff(self):
        """
        Find the difference between two dataframes
            return: rows from df1 not in df2, rows from df2 not in df1
        """
        # return rows from df1 not in df2
        df1_not_df2 = self.df1.select(self.req_cols).subtract(
            self.df2.select(self.req_cols)
        )
        # return rows from df2 not in df1
        df2_not_df1 = self.df2.select(self.req_cols).subtract(
            self.df1.select(self.req_cols)
        )
        return df1_not_df2, df2_not_df1

    def find_columns(self, columns, code):
        """
        Find the columns used in the code
            return: list of columns used in the code
        """
        # Define regular expressions to search for column names
        column_pattern = r"(?<!\w)[a-zA-Z_]+\.[a-zA-Z_]+(?!\w)"
        alias_pattern = r"as\s+([a-zA-Z_]+)"
        col_pattern = r"col\s*\(\s*['\"]([a-zA-Z_]+)['\"]\s*\)"

        # Search for column names in the code using regular expressions
        column_matches = re.findall(column_pattern, code)
        alias_matches = re.findall(alias_pattern, code)
        col_matches = re.findall(col_pattern, code)

        # Combine the matches and remove duplicates
        columns = set(column_matches + alias_matches + col_matches)

        # Return the list of column names (excluding dataframe and other variable names)
        columns = [
            col.split(".")[-1]
            for col in columns
            if col.split(".")[-1] not in {"df", "columns", "col"}
        ]

        columns = [col for col in columns if col in self.columns]

        return columns

    @staticmethod
    def check_is_spark_dataframe(df1, df2):
        """
        Check if the arguments are Spark DataFrames
        """
        if not isinstance(df1, DataFrame):
            raise ValueError("Argument 'df1' is not a Spark DataFrame")
        if not isinstance(df2, DataFrame):
            raise ValueError("Argument 'df2' is not a Spark DataFrame")
