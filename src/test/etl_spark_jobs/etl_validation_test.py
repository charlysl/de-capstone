from etl_spark_jobs.etl_spark_job_test_base import ETLSparkJobTestBase

from etl_spark_jobs.etl_validation import ETLValidation


class ETLValidationTest(ETLSparkJobTestBase):

    # test check_not_empty
    
    def test_not_empty_when_not_empty(self):
        df = self.create_df([['some_value']])
        validation = ETLValidation(df)
        validation.check_not_empty()
        pass

    def test_not_empty_when_empty(self):
        df = self.create_df([])
        validation = ETLValidation(df)
        self.assertRaises(
            ETLValidation.IsEmptyException,
            validation.check_not_empty
        )

    # test check_values_not_null

    def test_no_nulls_when_no_nulls(self):
        df = self.create_df([['some_value']])
        validation = ETLValidation(df)
        validation.check_no_nulls()
        pass

    def test_no_nulls_when_nulls(self):
        df = self.create_df([['some_value', None]])
        validation = ETLValidation(df)
        self.assertRaises(
            ETLValidation.HasNullsException,
            validation.check_no_nulls
        )

    # test check_no_duplicates

    def test_no_duplicates_when_no_dups(self):
        df = self.create_df([
            ['some_value', 'some_value'],
            ['some_value', 'some_other_value']
        ])
        validation = ETLValidation(df)
        validation.check_no_duplicates()
        pass

    def test_no_duplicates_when_dups(self):
        df = self.create_df([
            ['some_value'],
            ['some_value']
        ])
        validation = ETLValidation(df)
        self.assertRaises(
            ETLValidation.HasDuplicatesException,
            validation.check_no_duplicates
        )

    # test check_referential_integrity

    def test_referential_integrity_passes(self):
        df = self.create_df([
            ['some_value'],
            ['some_other_value'],
        ])
        validation = ETLValidation(df)
        validation.check_referential_integrity(df)
        pass

    def test_referential_integrity_fails(self):
        df = self.create_df([
            ['some_value'],
            ['some_other_value'],
        ])
        validation = ETLValidation(df)
        self.assertRaises(
            ETLValidation.ReferentialIntegrityException,
            validation.check_referential_integrity,
            df.limit(1)
        )

if __name__ == '__main__':
    import unittest
    unittest.main()

