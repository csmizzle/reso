"""
Fuzz together OFAC and GBR datasets
- Simple fuzzy matching across fields
- First use ids to match - quick wins??
- Fuzzy match against names 
- Fuzzy match against addresses
... can iterate but this feels alright to start
"""

import pyspark.pandas as ps
from pyspark.sql import Row, SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StopWordsRemover,
    Tokenizer,
    NGram,
    HashingTF,
    MinHashLSH,
    RegexTokenizer,
    SQLTransformer
)
from pyspark.sql import functions as F
from pyspark.sql import Window
import os
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


DIRPATH = os.path.dirname(os.path.realpath(__file__))


def flatten_exploded_dataframe(
    exploded_df: ps.DataFrame,
    exploded_column: str,
    subset_cols: list[str] = None,
    drop_cols: list[str] = None
) -> ps.DataFrame:
    """Cleaning utility to deconstruct Spark Row objects"""
    clean_rows = []
    for _, row in exploded_df.iterrows():
        if isinstance(row[exploded_column], Row):
            clean_rows.append(
                {**{'id': row['id']}, **row[exploded_column].asDict()}
            )
    result_df = ps.DataFrame(clean_rows)
    if subset_cols:
        result_df = result_df.drop_duplicates(subset=subset_cols)
    if drop_cols:
        result_df = result_df.drop(columns=drop_cols, axis=1)
    return result_df


def explode_flatten(
    df: ps.DataFrame,
    exploded_column: str,
    id_col: str = 'id',
    subset_cols: list[str] = None,
    drop_cols: list[str] = None
) -> ps.DataFrame:
    """Utility to explode a dataframe and deconstruct Spark Rows"""
    exploded_df = df[[id_col, exploded_column]].explode(exploded_column)
    return flatten_exploded_dataframe(
        exploded_df=exploded_df,
        exploded_column=exploded_column,
        subset_cols=subset_cols,
        drop_cols=drop_cols
    )


def address_cleaner(address_string):
    """Clean address parts from a Spark Row"""
    return [entry for entry in address_string.split(', ') if entry != '']


def clean_address_df(
    address_df: ps.DataFrame,
    address_col: str = 'value',
    pieces_thresh: int = 2
    ) -> ps.DataFrame:
    """Based on the parts an address, we can keep long and high fidelity addresses"""
    address_df[address_col] = address_df[address_col].apply(address_cleaner)
    address_df = address_df[address_df[address_col].str.len() > pieces_thresh]
    address_df[address_col] = address_df[address_col].apply(lambda x: ' '.join(x))
    return address_df


def create_fuzzy_pipeline(
    column_name: str,
    fit_df
) -> Pipeline:
    """Facroty method for a fuzzy string matching pipeline"""
    return (
        Pipeline(stages=[
            SQLTransformer(statement=f"SELECT *, lower({column_name}) lower_varaible FROM __THIS__"),
            Tokenizer(inputCol="lower_varaible", outputCol="token"),
            StopWordsRemover(inputCol="token", outputCol="stop"),
            SQLTransformer(statement="SELECT *, concat_ws(' ', stop) concat FROM __THIS__"),
            RegexTokenizer(pattern="", inputCol="concat", outputCol="char", minTokenLength=1),
            NGram(n=2, inputCol="char", outputCol="ngram"),
            HashingTF(inputCol="ngram", outputCol="vector"),
            MinHashLSH(inputCol="vector", outputCol="lsh", numHashTables=3)
        ]).fit(fit_df)
    )


def remove_ids(
    dataframe,
    id_col: str,
    ids: list
    ):
    """Cleaning utility for updating ids as we resolve new entities"""
    return dataframe[~dataframe[id_col].isin(ids)]


class Resolver:
    
    def __init__(
        self,
        input_df_a: ps.DataFrame,
        input_df_b: ps.DataFrame,
    ) -> None:
        self.input_df_a = input_df_a
        self.input_df_b = input_df_b
        self.resolved_data = ps.DataFrame()
    
    def _print_entities_remaining(self) -> None:
        print(f'DataFrame A: Entities remaining {len(self.input_df_a)}')
        print(f'DataFrame B: Entities remaining {len(self.input_df_b)}')

    def _id_join(
        self,
        explode_col: str,
        id_col: str,
        join_col: str
        ) -> ps.DataFrame:
        """Use ID column match shared UUIDs"""
        id_numbers_a= explode_flatten(
            df=self.input_df_a,
            exploded_column=explode_col,
            id_col=id_col
        )
        id_numbers_b= explode_flatten(
            df=self.input_df_b,
            exploded_column=explode_col,
            id_col=id_col
        )
        return id_numbers_a.merge(
            right=id_numbers_b,
            on=join_col
        ).rename(columns={'id_x': 'id_a', 'id_y': 'id_b'})

    def _update_id_reuslts(
        self,
        match_df: ps.DataFrame
    ) -> tuple[list, list]:
        """Update result DataFrame"""
        df_a_ids = list(match_df.id_a.values)
        df_b_ids = list(match_df.id_b.values)
        explanations = [
            f"ofac_id: {id_a} is a direct match with uk_id: {id_b} from the id_values column"
            for id_a, id_b in zip(df_a_ids, df_b_ids)
        ]
        id_results = ps.DataFrame([
            {'ofac_id': id_a, 'uk_id': id_b, 'explanation': explain}
            for id_a, id_b, explain in zip(df_a_ids, df_b_ids, explanations)
        ])
        self.resolved_data.append(id_results)
        return df_a_ids, df_b_ids
    
    @staticmethod
    def _fuzz_match(
        input_a: ps.DataFrame,
        input_b: ps.DataFrame,
        column_name: str,
        id_col: str = 'id',
        distance_thresh: float = 0.2,
    ) -> ps.DataFrame:
        """Use a simple fuzzy matching pipeline to match two string columns"""
        print('[!] Converting dataframes to spark ...')
        spark_a = input_a.to_spark()
        spark_b = input_b.to_spark()
        pipeline = create_fuzzy_pipeline(
            column_name=column_name,
            fit_df=spark_a
        )
        print(f'[!] Fitting for column: {column_name} ...')
        results_a = pipeline.transform(spark_a)
        results_a = results_a.filter(F.size(F.col("ngram")) > 0)
        results_b = pipeline.transform(spark_b)
        results_b = results_b.filter(F.size(F.col("ngram")) > 0)
        join_table = pipeline.stages[-1].approxSimilarityJoin(
            results_a,
            results_b,
            distance_thresh,
            "jaccardDist"
        )
        # select smallest distance for each id in dataframe using window function
        w = Window.partitionBy(f'datasetA.{id_col}')
        return (
            join_table
                .withColumn('minDist', F.min('jaccardDist').over(w))
                .where(F.col('jaccardDist') == F.col('minDist'))
                .drop('minDist')
        )

    def _update_fuzzy_results(
        self,
        join_table,
        string_col: str,
        id_col: str = 'id'
    ) -> tuple[list, list]:
        """Update resolved enties for fuzzy match pipline"""
        df_a_ids = list(join_table.select(F.col(f'datasetA.{id_col}').alias('id_a')).toPandas()['id_a'])
        df_b_ids = list(join_table.select(F.col(f'datasetB.{id_col}').alias('id_b')).toPandas()['id_b'])
        jaccard_scores = list(join_table.select(F.col(f'jaccardDist')).toPandas()['jaccardDist'])
        explanations = [
            f"ofac_id: {id_a} has a jaccard distance of {score} with uk_id: {id_b} from the {string_col} column"
            for id_a, id_b, score in zip(df_a_ids, df_b_ids, jaccard_scores)
        ]
        results = ps.DataFrame([
            {'ofac_id': id_a, 'uk_id': id_b, 'explanation': explain}
            for id_a, id_b, explain in zip(df_a_ids, df_b_ids, explanations)
        ])
        self.resolved_data = self.resolved_data.append(results)
        return df_a_ids, df_b_ids

    def resolve(
        self,
        distance_thresh: float = 0.2,
        address_pieces_threshold: int = 2
    ) -> ps.DataFrame:
        print('[!] Round One: Joining on IDs ...')
        # first round - id join
        id_results = self._id_join(
            explode_col="id_numbers",
            id_col='id',
            join_col='value'
        )
        # add ids and explanation to results
        df_a_ids, df_b_ids = self._update_id_reuslts(match_df=id_results)
        self.input_df_a = remove_ids(self.input_df_a, 'id', df_a_ids)
        self.input_df_b = remove_ids(self.input_df_b, 'id', df_b_ids)
        self._print_entities_remaining()
        # second round - name fuzzy matching
        print('[!] Round Two: Fuzzy Matching on Names ...')
        name_results = self._fuzz_match(
            input_a=self.input_df_a,
            input_b=self.input_df_b,
            column_name="name",
            distance_thresh=distance_thresh
        )
        # add ids from name results
        df_a_name_ids, df_b_name_ids = self._update_fuzzy_results(
            join_table=name_results,
            string_col="name",
        )
        self.input_df_a = remove_ids(self.input_df_a, 'id', df_a_name_ids)
        self.input_df_b = remove_ids(self.input_df_b, 'id', df_b_name_ids)
        self._print_entities_remaining()
        # third round - resolve against addresses
        # there is one additional step here to clean addresses
        print('[!] Round Three: Fuzzy Matching on Addresses ...')
        input_addresses_a = explode_flatten(
            self.input_df_a,
            exploded_column="addresses"
        )
        input_addresses_a = clean_address_df(
            input_addresses_a,
            pieces_thresh=address_pieces_threshold
        )
        input_addresses_b = explode_flatten(
            self.input_df_b,
            exploded_column="addresses"
        )
        input_addresses_b = clean_address_df(
            input_addresses_b,
            pieces_thresh=address_pieces_threshold
        )
        address_results = self._fuzz_match(
            input_a=input_addresses_a,
            input_b=input_addresses_b,
            column_name="value",
            distance_thresh=distance_thresh
        )
        # add ids from address results
        df_a_addresses_ids, df_b_addresses_ids = self._update_fuzzy_results(
            join_table=address_results,
            string_col="addresses",
        )
        self.input_df_a = remove_ids(self.input_df_a, 'id', df_a_addresses_ids)
        self.input_df_b = remove_ids(self.input_df_b, 'id', df_b_addresses_ids)
        self._print_entities_remaining()
        return self.resolved_data


if __name__ == "__main__":
    gbr_pf = ps.read_json(os.path.join(DIRPATH, 'data', 'gbr.jsonl'), lines=True)
    ofac_pf = ps.read_json(os.path.join(DIRPATH, 'data', 'ofac.jsonl'), lines=True)
    resolver = Resolver(
        input_df_a=ofac_pf,
        input_df_b=gbr_pf
    )
    resolved_df = resolver.resolve(
        distance_thresh=0.25,  # jaccard distance cutoff
        address_pieces_threshold=2  # any addresses that have just city, country will be excluded in Round Three
    )
    resolved_df.to_pandas().to_csv(os.path.join(DIRPATH, 'data', 'output.csv'), index=False)
