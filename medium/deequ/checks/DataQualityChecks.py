from pyspark.sql import SparkSession, Row
import pydeequ
from pydeequ.checks import *
from pydeequ.verification import *
spark = (SparkSession
    .builder
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .getOrCreate())

df = spark.sparkContext.parallelize([
            Row(a="foo", b=1, c=5),
            Row(a="bar", b=2, c=6),
            Row(a="baz", b=3, c=None)]).toDF()

check = Check(spark, CheckLevel.Warning, "Review Check")
deequ_rules = [
    {"col_check_type": "hasSize", "assertion": "lambda x: x==3"},
    {"column_name": "a", "col_check_type": "hasMin", "assertion": "lambda x: x==0"},
    {"column_name": "c", "col_check_type": "isComplete"},
    {"column_name": "a", "col_check_type": "isUnique"},
    {"column_name": "a", "col_check_type": "isContainedIn","allowedValues":["foo", "bar", "baz"]},
    {"column_name": "b", "col_check_type": "isNonNegative"},
]
#pass the constraints dynamically to the check module
for rule in deequ_rules:

    if "allowedValues" in rule:
        getattr(check, rule.get('col_check_type'))(rule.get('column_name'),rule.get('allowedValues'))
    #for hasSize checks which donot have column param
    elif 'column_name' not in rule:
        getattr(check, rule.get('col_check_type'))(eval(rule.get('assertion')))
    #column checks with lambda assertion
    elif "assertion" in rule: \
        getattr(check, rule.get('col_check_type'))(rule.get('column_name'), eval(rule.get('assertion')))
    else:
        getattr(check, rule.get('col_check_type'))(rule.get('column_name'))


checkResult = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(check).run()

checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.show()

spark.sparkContext._gateway.shutdown_callback_server()
spark.stop()