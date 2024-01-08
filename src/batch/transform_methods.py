from project_utils import InputData
from pyspark.sql.functions import col, row_number, split
from pyspark.sql.window import Window
from project_utils import Analysis

df_result_dict = {}

def transformation_for_analysis_1(spark_client, analysis_config):
    print("Transformation Started for Analysis 1 =>")

    df_primary_person = spark_client.read_spark_table(InputData.Primary_Person.value)

    df_primary_person_filtered = df_primary_person.filter((df_primary_person.PRSN_GNDR_ID == 'MALE') & (df_primary_person.PRSN_INJRY_SEV_ID == 'KILLED'))
    df_grouped = df_primary_person_filtered.groupby("crash_id").count().withColumnRenamed("count", "tot_count")
    df_final = df_grouped.filter(df_grouped.tot_count > 2)
    tot_count = df_final.count()
    df_result_dict[Analysis.Analysis_1.value] = tot_count
    return df_result_dict

def transformation_for_analysis_2(spark_client, analysis_config):
    print("Transformation Started for Analysis 2 =>")

    df_units = spark_client.read_spark_table(InputData.Units.value)
    df_units_filtered = df_units.filter((df_units.VEH_BODY_STYL_ID == 'MOTORCYCLE') | (df_units.VEH_BODY_STYL_ID == 'POLICE MOTORCYCLE'))
    tot_count = df_units_filtered.count()
    df_result_dict[Analysis.Analysis_2.value] = tot_count
    return df_result_dict

def transformation_for_analysis_3(spark_client, analysis_config):
    print("Transformation Started for Analysis 3 =>")

    df_units = spark_client.read_spark_table(InputData.Units.value)
    df_primary_person = spark_client.read_spark_table(InputData.Primary_Person.value)

    df_primary_person_filtered = df_primary_person.filter((df_primary_person.PRSN_AIRBAG_ID == 'NOT DEPLOYED') &\
                                                          (df_primary_person.PRSN_INJRY_SEV_ID == 'KILLED') &\
                                                          (df_primary_person.PRSN_TYPE_ID == 'DRIVER'))
    df_joined = df_units.join(df_primary_person_filtered, ["CRASH_ID", "UNIT_NBR"], "inner")
    df_final = df_joined.groupBy("VEH_MAKE_ID").count().orderBy("count", ascending=False).limit(5)
    df_result_dict[Analysis.Analysis_3.value] = df_final
    return df_result_dict

def transformation_for_analysis_4(spark_client, analysis_config):
    print("Transformation Started for Analysis 4 =>")

    df_units = spark_client.read_spark_table(InputData.Units.value)
    df_primary_person = spark_client.read_spark_table(InputData.Primary_Person.value)

    df_units_filtered = df_units.filter(df_units.VEH_HNR_FL == 'Y')
    df_pp_filtered = df_primary_person.filter(((df_primary_person.DRVR_LIC_TYPE_ID == 'COMMERCIAL DRIVER LIC.') | \
                                              (df_primary_person.DRVR_LIC_TYPE_ID == 'DRIVER LICENSE')) \
                                              & (df_primary_person.PRSN_TYPE_ID.ilike('DRIVER%')))
    df_final = df_units_filtered.join(df_pp_filtered, ["CRASH_ID", "UNIT_NBR"], "inner")
    df_result_dict[Analysis.Analysis_4.value] = df_final.count()
    return df_result_dict

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
    df_result_dict[Analysis.Analysis_5.value] = df_final_2
    return df_result_dict

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
    df_result_dict[Analysis.Analysis_6.value] = df_final
    return df_result_dict

def transformation_for_analysis_7(spark_client, analysis_config):
    print("Transformation Started for Analysis 7 =>")

    df_units = spark_client.read_spark_table(InputData.Units.value)
    df_primary_person = spark_client.read_spark_table(InputData.Primary_Person.value)
    df_joined = df_units.join(df_primary_person, ["CRASH_ID", "UNIT_NBR"], "inner")
    df_grouped = df_joined.groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count()
    window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
    df_final = df_grouped.withColumn("rn", row_number().over(window_spec)).where("rn = 1").drop("rn")
    df_result_dict[Analysis.Analysis_7.value] = df_final
    return df_result_dict

def transformation_for_analysis_8(spark_client, analysis_config):
    print("Transformation Started for Analysis 8 =>")

    df_units = spark_client.read_spark_table(InputData.Units.value)
    df_units_filtered = df_units.\
                        filter("CONTRIB_FACTR_1_ID == 'HAD BEEN DRINKING' or CONTRIB_FACTR_2_ID = 'HAD BEEN DRINKING' or CONTRIB_FACTR_P1_ID = 'HAD BEEN DRINKING'")
    df_primary_person = spark_client.read_spark_table(InputData.Primary_Person.value)
    df_primary_person_filtered = df_primary_person.filter(df_primary_person.PRSN_TYPE_ID.ilike('DRIVER%'))
    df_joined = df_units_filtered.join(df_primary_person_filtered, ["CRASH_ID", "UNIT_NBR"], "inner")
    df_final = df_joined.filter(df_joined.DRVR_ZIP.isNotNull()).groupby("DRVR_ZIP").count().orderBy("count", ascending=False).limit(5)
    df_result_dict[Analysis.Analysis_8.value] = df_final
    return df_result_dict

def transformation_for_analysis_9(spark_client, analysis_config):
    print("Transformation Started for Analysis 9 =>")

    df_units = spark_client.read_spark_table(InputData.Units.value)
    df_damages = spark_client.read_spark_table(InputData.Damages.value)

    df_units_filtered_1 = df_units.\
                        filter(df_units.VEH_DMAG_SCL_1_ID.ilike('DAMAGED%')).\
                        withColumn("damage_1", split("VEH_DMAG_SCL_1_ID", ' ').getItem(1)).\
                        filter("damage_1 > 4").select("crash_id", "unit_nbr", "fin_resp_type_id")

    df_units_filtered_2 = df_units. \
        filter(df_units.VEH_DMAG_SCL_2_ID.ilike('DAMAGED%')). \
        withColumn("damage_2", split("VEH_DMAG_SCL_2_ID", ' ').getItem(1)). \
        filter("damage_2 > 4").select("crash_id", "unit_nbr", "fin_resp_type_id")

    df_units_filtered = df_units_filtered_1.union(df_units_filtered_2).\
                        distinct().\
                        filter("fin_resp_type_id like '%INSURANCE%'")

    df_units_crash_id_distinct = df_units_filtered.select("crash_id").distinct()
    df_damages_crash_id_distinct = df_damages.select("crash_id").distinct()

    df_final = df_units_crash_id_distinct.join(df_damages_crash_id_distinct, ["crash_id"], "leftanti")
    df_result_dict[Analysis.Analysis_9.value] = df_final.count()
    return df_result_dict

def transformation_for_analysis_10(spark_client, analysis_config):
    print("Transformation Started for Analysis 10 =>")

    df_units = spark_client.read_spark_table(InputData.Units.value)
    df_units.cache() # as this df is used multiple times
    df_charges = spark_client.read_spark_table(InputData.Charges.value)
    df_primary_person = spark_client.read_spark_table(InputData.Primary_Person.value)

    top_10_color = df_units.filter(df_units.VEH_COLOR_ID != 'NA').\
                   groupBy('VEH_COLOR_ID').count().\
                   orderBy("count", ascending = False).limit(10).select("VEH_COLOR_ID")

    top_25_state = df_units.filter(df_units.VEH_LIC_STATE_ID != 'NA').\
                    groupBy("VEH_LIC_STATE_ID").count().\
                    orderBy("count", ascending = False).limit(25).select("VEH_LIC_STATE_ID")

    df_pp_filtered = df_primary_person.filter(((df_primary_person.DRVR_LIC_TYPE_ID == 'COMMERCIAL DRIVER LIC.') | \
                                             (df_primary_person.DRVR_LIC_TYPE_ID == 'DRIVER LICENSE')) \
                                              & (df_primary_person.PRSN_TYPE_ID.ilike('DRIVER%')))

    df_charges_filtered = df_charges.filter(df_charges.CHARGE.ilike('%SPEED%'))

    df_temp = df_pp_filtered.join(df_charges_filtered, ["CRASH_ID", "UNIT_NBR", "PRSN_NBR"], "inner")

    df_temp_2 = df_units.join(df_temp, ["CRASH_ID", "UNIT_NBR"], "inner")

    df_intermediate = df_temp_2.join(top_10_color, ["VEH_COLOR_ID"], "inner")

    df_final = df_intermediate.join(top_25_state, ["VEH_LIC_STATE_ID"], "inner")

    df_result = df_final.groupBy("VEH_MAKE_ID").count().\
                orderBy("count", ascending = False).limit(5)

    df_result_dict[Analysis.Analysis_10.value] = df_result

    return df_result_dict

def transformation_for_all_analysis(spark_client, analysis_config):
    print("Transformation Started for All the Analysis =>")

    transformation_for_analysis_1(spark_client, analysis_config)
    transformation_for_analysis_2(spark_client, analysis_config)
    transformation_for_analysis_3(spark_client, analysis_config)
    transformation_for_analysis_4(spark_client, analysis_config)
    transformation_for_analysis_5(spark_client, analysis_config)
    transformation_for_analysis_6(spark_client, analysis_config)
    transformation_for_analysis_7(spark_client, analysis_config)
    transformation_for_analysis_8(spark_client, analysis_config)
    transformation_for_analysis_9(spark_client, analysis_config)
    transformation_for_analysis_10(spark_client, analysis_config)

    return df_result_dict



