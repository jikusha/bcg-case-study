from project_utils import InputData
from pyspark.sql.functions import col

def transformation_for_analysis_1(spark_client, analysis_config):
    print("Transformation Started for Analysis 1 =>")

    df_primary_person = spark_client.read_spark_table(InputData.Primary_Person.value)

    # df_analysis_1 = df_primary_person.filter("PRSN_GNDR_ID == 'MALE' and PRSN_INJRY_SEV_ID == 'KILLED'").groupby("crash_id").count().where("count > 2")
    df_primary_person_filtered = df_primary_person.filter((df_primary_person.PRSN_GNDR_ID == 'MALE') & (df_primary_person.PRSN_INJRY_SEV_ID == 'KILLED'))
    df_grouped = df_primary_person_filtered.groupby("crash_id").count().withColumnRenamed("count", "tot_count")
    df_final = df_grouped.filter(df_grouped.tot_count > 2)
    tot_count = df_final.count()
    return tot_count