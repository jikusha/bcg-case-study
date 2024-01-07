from project_utils import InputData
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

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
    print("Transformation Started for Analysis 3 =>")

    df_units = spark_client.read_spark_table(InputData.Units.value)
    df_primary_person = spark_client.read_spark_table(InputData.Primary_Person.value)

    df_primary_person_filtered = df_primary_person.filter((df_primary_person.PRSN_AIRBAG_ID == 'NOT DEPLOYED') &\
                                                          (df_primary_person.PRSN_INJRY_SEV_ID == 'KILLED') &\
                                                          (df_primary_person.PRSN_TYPE_ID == 'DRIVER'))
    df_joined = df_units.join(df_primary_person_filtered, ["CRASH_ID", "UNIT_NBR"], "inner")
    df_final = df_joined.groupBy("VEH_MAKE_ID").count().orderBy("count", ascending=False).limit(5)

    return df_final

def transformation_for_analysis_4(spark_client, analysis_config):
    print("Transformation Started for Analysis 4 =>")

    df_units = spark_client.read_spark_table(InputData.Units.value)
    df_primary_person = spark_client.read_spark_table(InputData.Primary_Person.value)

    df_units_filtered = df_units.filter(df_units.VEH_HNR_FL == 'Y')
    df_pp_filtered = df_primary_person.filter(((df_primary_person.DRVR_LIC_TYPE_ID == 'COMMERCIAL DRIVER LIC.') | \
                                              (df_primary_person.DRVR_LIC_TYPE_ID == 'DRIVER LICENSE')) \
                                              & (df_primary_person.PRSN_TYPE_ID.like('DRIVER%')))
    df_final = df_units_filtered.join(df_pp_filtered, ["CRASH_ID", "UNIT_NBR"], "inner")
    return df_final.count()

def transformation_for_analysis_5(spark_client, analysis_config):
    print("Transformation Started for Analysis 5 =>")

    df_units = spark_client.read_spark_table(InputData.Units.value)
    df_primary_person = spark_client.read_spark_table(InputData.Primary_Person.value)

    df_pp_filtered = df_primary_person.filter(df_primary_person.PRSN_GNDR_ID == 'FEMALE').select("crash_id").distinct()
    df_joined = df_units.join(df_pp_filtered, df_units.CRASH_ID == df_pp_filtered.crash_id, "leftanti")
    # as I am not able to find the state column in the dataset where crash is occured, so I am taking
    # VEH_LIC_STATE_ID column as state
    df_final = df_joined.groupBy("VEH_LIC_STATE_ID").count().orderBy("count", ascending = False).limit(1)
    df_final_2 = df_final.withColumnRenamed("VEH_LIC_STATE_ID", "State")
    return df_final_2

def transformation_for_analysis_6(spark_client, analysis_config):
    print("Transformation Started for Analysis 6 =>")

    df_units = spark_client.read_spark_table(InputData.Units.value)
    df_primary_person = spark_client.read_spark_table(InputData.Primary_Person.value)
    df_pp_filtered = df_primary_person.filter((df_primary_person['NON_INJRY_CNT'] != 1) \
                                                & (df_primary_person['UNKN_INJRY_CNT'] != 1))
    df_joined = df_units.join(df_pp_filtered, ["CRASH_ID", "UNIT_NBR"], "inner")
    df_grouped = df_joined.groupBy("VEH_MAKE_ID").count().orderBy("count", ascending=False)
    window_spec = Window.orderBy(col("count").desc())
    df_final = df_grouped.withColumn("rn", row_number().over(window_spec)).where("rn>=3 and rn<=5").drop("rn")
    return df_final

def transformation_for_analysis_7(spark_client, analysis_config):
    print("Transformation Started for Analysis 7 =>")

    df_units = spark_client.read_spark_table(InputData.Units.value)
    df_primary_person = spark_client.read_spark_table(InputData.Primary_Person.value)
    df_joined = df_units.join(df_primary_person, ["CRASH_ID", "UNIT_NBR"], "inner")
    df_grouped = df_joined.groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count()
    window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
    df_final = df_grouped.withColumn("rn", row_number().over(window_spec)).where("rn = 1").drop("rn")
    return df_final

def transformation_for_analysis_8(spark_client, analysis_config):
    print("Transformation Started for Analysis 8 =>")

    df_units = spark_client.read_spark_table(InputData.Units.value)
    df_units_filtered = df_units.\
                        filter("CONTRIB_FACTR_1_ID == 'HAD BEEN DRINKING' or CONTRIB_FACTR_2_ID = 'HAD BEEN DRINKING' or CONTRIB_FACTR_P1_ID = 'HAD BEEN DRINKING'")
    df_primary_person = spark_client.read_spark_table(InputData.Primary_Person.value)
    df_primary_person_filtered = df_primary_person.filter(df_primary_person.PRSN_TYPE_ID.like('DRIVER%'))
    df_joined = df_units_filtered.join(df_primary_person_filtered, ["CRASH_ID", "UNIT_NBR"], "inner")
    df_final = df_joined.filter(df_joined.DRVR_ZIP.isNotNull()).groupby("DRVR_ZIP").count().orderBy("count", ascending=False).limit(5)
    return df_final
