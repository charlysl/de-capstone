"""
The purporse of this job is to just detect that Spark is
ready to execute jobs.

I have found that if steps are submitted straightaway when
starting the EMR cluster, they all fail with all kinds of
different errors, like dependencies are missing.

So a solution would be to just submit this step right
after the EMR cluster is started, and, only when this
do-nothing job suceeds, submit other steps.

However, have to at least create a spark session, otherwise
the cluster will fail this job.
"""

from datalake.utils import spark_helper

spark = spark_helper.get_spark()

# do nothing