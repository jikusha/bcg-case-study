from project_utils import InputData
from pyspark.sql.functions import col

def transformation_for_analysis_1(spark_client, analysis_config):
    print("Transformation Started for Analysis 1 =>")

    df_primary_person = spark_client.read_spark_table(InputData.Primary_Person.value)

    df_primary_person_filtered = df_primary_person.filter((df_primary_person.PRSN_GNDR_ID == 'MALE') & (df_primary_person.PRSN_INJRY_SEV_ID == 'KILLED'))
    df_grouped = df_primary_person_filtered.groupby("crash_id").count().withColumnRenamed("count", "tot_count")
    df_final = df_grouped.filter(df_grouped.tot_count > 2)
    tot_count = df_final.count()
    return tot_count

def transformation_for_analysis_2(spark_client, analysis_config):
    print("Transformation Started for Analysis 2 =>")

    df_units = spark_client.read_spark_table(InputData.Units.value)

    df_units_filtered = df_units.filter((df_units.VEH_BODY_STYL_ID == 'MOTORCYCLE') | (df_units.VEH_BODY_STYL_ID == 'POLICE MOTORCYCLE'))
    tot_count = df_units_filtered.count()
    return tot_count

def transformation_for_analysis_3(spark_client, analysis_config):
    # df_pp_filtered = df_pp.dropDuplicates(["CRASH_ID", "UNIT_NBR", "PRSN_NBR"]).filter("PRSN_AIRBAG_ID == 'NOT DEPLOYED' and PRSN_INJRY_SEV_ID == 'KILLED' and PRSN_TYPE_ID == 'DRIVER'")

    # df_joined = df_units.join(df_pp_filtered, ["CRASH_ID", "UNIT_NBR"], "inner").select(df_units.CRASH_ID, df_units.UNIT_NBR, df_pp_filtered.PRSN_NBR, "PRSN_TYPE_ID", "VEH_MAKE_ID", "PRSN_INJRY_SEV_ID", "PRSN_AIRBAG_ID")
    # df_joined_group = df_joined.groupBy("VEH_MAKE_ID").count().orderBy("count", ascending = False).limit(5)
    #
    # df_joined_group.show()

    print("Transformation Started for Analysis 3 =>")

    df_units = spark_client.read_spark_table(InputData.Units.value)
    df_primary_person = spark_client.read_spark_table(InputData.Primary_Person.value)

    df_primary_person_filtered = df_primary_person.filter((df_primary_person.PRSN_AIRBAG_ID == 'NOT DEPLOYED') &\
                                                          (df_primary_person.PRSN_INJRY_SEV_ID == 'KILLED') &\
                                                          (df_primary_person.PRSN_TYPE_ID == 'DRIVER'))
    df_joined = df_units.join(df_primary_person_filtered, ["CRASH_ID", "UNIT_NBR"], "inner")
    df_final = df_joined.groupBy("VEH_MAKE_ID").count().orderBy("count", ascending=False).limit(5)

    return df_final