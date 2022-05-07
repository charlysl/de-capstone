from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer

import pyspark.sql.functions as F

class Stopwords():
    """
    Description: create a list of stopwords from corpora extracted from Spark dataframes
        - the dataframes must have a column called 'text'
        
    Example:
    ```
    (
        Stopwords([
            df1,
            df2,
        ])
        .stopwords(50)
    )
    ```
    """
    #see https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.StopWordsRemover.html
    
    # split at all punctuation marks and whitespace (but not ')
    token_separator = "\\s|\\.|\\,|\\-|/|!|@|#|\\$|%|&|\\(|\\)|\\|\\[|\\]|\"|:|;"
    
    def __init__(self, dfs):
        self.dfs = dfs

    def get_model(self, df):
        return (
            Pipeline(stages=[
                RegexTokenizer(
                    pattern=self.token_separator,
                    inputCol="text",
                    outputCol="tokens",
                    minTokenLength=3
                ),
            ])
            .fit(df)
        )
    
    def tokens(self, df):
        return self.get_model(df).stages[0].transform(df)
    
    def words(self, df):
        tokens = tokens = self.tokens(df)
        return tokens.select(F.explode(F.col('tokens')).alias('word'))
    
    def all_words(self):
        dfs = self.dfs
        all_words = self.words(dfs[0])
        for df in self.dfs[1:]:
            all_words = all_words.union(self.words(df))
        return all_words
    
    def freqs(self, df):
        return (
            df
            .groupby('word')
            .count()
            .sort(F.desc(F.col('count')))
        )
    
    def limit_words(self, df, num_words):
        return df.select('word').limit(num_words)
    
    def to_list(self, df):
        return [word['word'] for word in df.collect()]
    
    def stopwords(self, num_words):
        """
        Description: generate stop words
        
        Parameters:
        - num_words: number of stop words to generate
        
        Returns: a list of num_words stop words
        """
        all_words = self.all_words()
        ordered = self.freqs(all_words)
        subset = self.limit_words(ordered, num_words)
        return self.to_list(subset)


