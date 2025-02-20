import epitran
import pandas as pd
from pathlib import Path
import argparse
import sys
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs


class GenerateIPATransliterations():
    """
    Generates IPA transliterations for the columns: [wrongword, rightword]
    Uses a cache based approach, by reusing the computed values stored in ipa_meta_data_df

    Input:
        - w2r_df: The DF which has the columns whose IPA-transliterations are to be generated
        - ipa_meta_data: The DF which has pre-computed values for all past "words"

    Output:
        - w2r_ipa_df: The DF with two new columns which are [ipa_wrongword, ipa_rightword]
        - ipa_meta_data: The updated cacheDF that has the extra words which were newly computed in this run.

    Algorithm:
        - Cache and Compute for "rightword" column
        - Cache and Compute for "wrongword" column using the updated cache.
        - Write the modified DF as well as the updated cacheDF
    """

    w2r_df = None
    w2r_ipa_df = None
    ipa_meta_data = None
    max_ipa_computations = None

    def __init__(self,max_ipa_computations=10000,w2r_path=None,ipa_meta_data_path=None,w2r_scored_path=None,ipa_meta_data_output_path=None):
        
        #filesystems
        self.fs = fs.LocalFileSystem()
        self.hdfs = fs.HadoopFileSystem("default")
        self.temp_dir = "/tmp"
        
        # inputs
        self.max_ipa_computations = max_ipa_computations
        self.w2r_path = w2r_path
        self.ipa_meta_data_path = ipa_meta_data_path

        # outputs
        self.w2r_scored_path = w2r_scored_path
        self.ipa_meta_data_output_path = ipa_meta_data_output_path

    def local_to_hdfs(self,local_path,hdfs_path):
        fs.copy_file(
            source=local_path,
            destination=hdfs_path,
            source_filesystem=self.fs,
            destination_filesystem=self.hdfs
        )

    def load(self):
        self.w2r_df = pq.ParquetDataset(self.w2r_path,self.hdfs).read().to_pandas()
        self.ipa_meta_data = pq.ParquetDataset(self.ipa_meta_data_path,self.hdfs).read().to_pandas()

    def transform(self):
        self.w2r_ipa_df = self.w2r_df.copy()
        self.w2r_ipa_df, self.ipa_meta_data = self.cache_and_compute(self.w2r_ipa_df,
                                                                     self.ipa_meta_data,
                                                                     "rightword",
                                                                     "word",
                                                                     "ipa_transliteration"
                                                                     )
        self.w2r_ipa_df = self.w2r_ipa_df.rename(columns={"ipa_transliteration": "ipa_rightword"})

        self.w2r_ipa_df, self.ipa_meta_data = self.cache_and_compute(self.w2r_ipa_df,
                                                                     self.ipa_meta_data,
                                                                     "wrongword",
                                                                     "word",
                                                                     "ipa_transliteration"
                                                                     )
        self.w2r_ipa_df = self.w2r_ipa_df.rename(columns={"ipa_transliteration": "ipa_wrongword"})

    def save(self):
        # Save the final DFs to HDFS
        self.w2r_ipa_df.to_parquet(self.temp_dir + "/w2r_ipa_df.parquet")
        self.local_to_hdfs(self.temp_dir + "/w2r_ipa_df.parquet",self.w2r_scored_path)
        self.ipa_meta_data.to_parquet(self.temp_dir + "/ipa_meta_data.parquet")
        self.local_to_hdfs(self.temp_dir + "/ipa_meta_data.parquet",self.ipa_meta_data_output_path)

    def test(self):

        test_cases = [
            #T1: Exactly two extra columns are created
            [self.w2r_ipa_df.shape[1] == self.w2r_df.shape[1] + 2, "#T1: Incorrect #columns in final DF"],

            #T2: Number of rows should be maintained
            [self.w2r_ipa_df.shape[0] == self.w2r_df.shape[0], "#T2: Input and output #rows not matching"]
        ]

        self.run_test_cases(test_cases)

    def cache_and_compute(self, df, cache_df, left_key_col, right_key_col, cache_col):
        """
        Caches and computes IPA transcriptions for a column in a DF using a cacheDF

        Args:
            df (pandas.DataFrame): The original DataFrame with data to be scored.
            cache_df (pandas.DataFrame): The cacheDF containing precomputed values.
            left_key_col (str): The column in the original DF to match with the cache.
            right_key_col (str): The column in the cacheDF to match with the original data.
            cache_col (str): The column in cacheDF that stores the cached IPA transcriptions.

        Returns:
            pandas.DataFrame: A DataFrame with IPA transcriptions added based on the cache.
            pandas.DataFrame: An updated cache DataFrame.

        Raises:
            Exception: If too many new values need to be computed (manual intervention required).
        """
        # Create a local epitran object
        epi = epitran.Epitran('eng-Latn')

        # Merge the original DataFrame with the cache DataFrame based on the key column
        merged_df = pd.merge(
            df, cache_df, left_on=left_key_col, right_on=right_key_col, how='left'
        ).drop(columns=[right_key_col])

        # Find rows where cache is missing
        missing_cache = merged_df[merged_df[cache_col].isnull()]

        if missing_cache.drop_duplicates(subset=[left_key_col]).shape[0] > self.max_ipa_computations:
            missing_cache_path = str(Path(self.input_base_dir_path) / "ComputeIPATransliterationsManually")
            DFToParquet.put_df(missing_cache_path,
                               missing_cache.drop_duplicates(subset=[left_key_col])[[left_key_col]].rename(
                                   columns={left_key_col: "word"}))
            raise Exception("Too many new values to be computed "
                            f"({missing_cache.drop_duplicates(subset=[left_key_col]).shape[0]}). "
                            f"Manual intervention required. Missing words are stored at: {missing_cache_path}")
        elif not missing_cache.empty:
            local_cache = dict()
            def get_or_update_local_cache(word):
                if word not in local_cache:
                    local_cache[word] = epi.transliterate(word)
                return local_cache[word]

            # Calculate scores for the rows with missing cache
            missing_cache[cache_col] = missing_cache[left_key_col].apply(get_or_update_local_cache)
            # Update the cache DataFrame with the newly computed values
            cache_df = pd.concat([cache_df,
                                  pd.DataFrame(list(local_cache.items()), columns=[right_key_col, cache_col])],
                                 ignore_index=True)
            # Create the final scored DataFrame by union
            merged_df = pd.concat([merged_df[merged_df[cache_col].notnull()],
                                   missing_cache],
                                  ignore_index=True)

        # Return the scored DataFrame and updated cache DataFrame
        return merged_df, cache_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate IPA Transliterations')
    parser.add_argument('--w2r_path', type=str)
    parser.add_argument('--ipa_meta_data_path', type=str)
    parser.add_argument('--w2r_scored_path', type=str)
    parser.add_argument('--ipa_meta_data_output_path', type=str)
    parser.add_argument('--max_ipa_computations', type=int, default=10000)

    args = parser.parse_args()

    g = GenerateIPATransliterations(max_ipa_computations=args.max_ipa_computations,
                                    w2r_path=args.w2r_path,
                                    ipa_meta_data_path=args.ipa_meta_data_path,
                                    w2r_scored_path=args.w2r_scored_path,
                                    ipa_meta_data_output_path=args.ipa_meta_data_output_path)

    g.load()
    g.transform()
    g.test()
    g.save()
