import run_everything


def test_run_everything(glueContext):
    spark = glueContext.spark_session
    run_everything.run(spark)