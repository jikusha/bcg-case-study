from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, split

spark = SparkSession.builder \
    .appName("BCG Case Study") \
    .master('local[*]') \
    .getOrCreate()


# df_charges = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("../Data/Charges_use.csv")
# df_charges.show(10)
# print(df_charges.count())

# df_damages = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("../Data/Damages_use.csv")
# df_damages.show(10)
# print(df_damages.count())

# df_endorse = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("../Data/Endorse_use.csv")
# df_endorse.show(10)
# print(df_endorse.count())

df_pp = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("input_data/Primary_Person_use.csv")
df_pp = df_pp.dropDuplicates(["CRASH_ID", "UNIT_NBR", "PRSN_NBR"])
# df_pp.select("PRSN_TYPE_ID", "DRVR_LIC_TYPE_ID").distinct().show(truncate=False)
# df_pp.show()
# print(df_pp.count())
# print(df_pp.dropDuplicates(["CRASH_ID", "UNIT_NBR", "PRSN_NBR"]).count())
# df_pp.select("PRSN_NBR").distinct().show()
# df_pp.select("PRSN_INJRY_SEV_ID","DEATH_CNT").distinct().show(100)
# df_pp.filter("PRSN_GNDR_ID == 'MALE' and PRSN_INJRY_SEV_ID == 'KILLED'").orderBy("CRASH_ID").select("crash_id","PRSN_GNDR_ID", "PRSN_INJRY_SEV_ID").show(100)
# df_pp.filter("PRSN_GNDR_ID == 'MALE' and PRSN_INJRY_SEV_ID == 'KILLED'").groupby("crash_id").count().orderBy("CRASH_ID").show(100)
# print(df_pp.count())

# df_restrict = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("../Data/Restrict_use.csv")
# df_restrict.show(truncate=False)
# print(df_restrict.count())


df_units = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("input_data//Units_use.csv")
# df_units.createOrReplaceTempView("units")
df_units = df_units.dropDuplicates(["CRASH_ID", "UNIT_NBR"])
# df_units.show()
# df_units.select("UNIT_DESC_ID").distinct().show()
# df_units.filter("UNIT_DESC_ID != 'MOTOR VEHICLE'").select("UNIT_DESC_ID", "VEH_LIC_STATE_ID").distinct().show()
# print(df_units.count())
# print(df_units.dropDuplicates().count())
# print(df_units.dropDuplicates(["CRASH_ID", "UNIT_NBR"]).count())
# spark.sql("select crash_id, UNIT_NBR, count(*) from units group by crash_id, UNIT_NBR having count(*)>1").show()
# df_units.filter("crash_id = '14851488' and UNIT_NBR = 2").show(truncate=False)
# df_units.select("VEH_BODY_STYL_ID").distinct().show()
# df_units.select("UNIT_DESC_ID", "VEH_BODY_STYL_ID").distinct().show(100,truncate=False)
# df_units.filter("CRASH_ID == '15474337'").orderBy("UNIT_NBR").show()
# df_units.show(100, truncate=False)
# df_units.select("UNIT_NBR").dropDuplicates().show()
# print(df_units.count())

#1
# df_pp.filter("PRSN_GNDR_ID == 'MALE' and PRSN_INJRY_SEV_ID == 'KILLED'").orderBy("CRASH_ID").select("crash_id","PRSN_GNDR_ID", "PRSN_INJRY_SEV_ID")
# df_analytics_1 = df_pp.filter("PRSN_GNDR_ID == 'MALE' and PRSN_INJRY_SEV_ID == 'KILLED'").groupby("crash_id").count().where("count > 2")
# print(df_analytics_1.count())


#2
# df_2 = df_units.where("VEH_BODY_STYL_ID == 'MOTORCYCLE' or VEH_BODY_STYL_ID == 'POLICE MOTORCYCLE'")
# print(df_2.count())
# print(df_2.dropDuplicates(["CRASH_ID", "VIN"]).count())
# print(df_2.dropDuplicates().count())


#3
# df_pp_filtered = df_pp.dropDuplicates(["CRASH_ID", "UNIT_NBR", "PRSN_NBR"]).filter("PRSN_AIRBAG_ID == 'NOT DEPLOYED' and PRSN_INJRY_SEV_ID == 'KILLED' and PRSN_TYPE_ID == 'DRIVER'")
# print(df_pp_filtered.count())
# df_units = df_units.dropDuplicates(["CRASH_ID", "UNIT_NBR"])

# df_pp.select("PRSN_AIRBAG_ID").distinct().show()
# df_pp.select("PRSN_INJRY_SEV_ID").distinct().show()
# df_pp.select("PRSN_TYPE_ID").distinct().show()

# df_pp.orderBy("CRASH_ID", "UNIT_NBR", "PRSN_NBR").show()
# df_units.orderBy("CRASH_ID", "UNIT_NBR").show()

# df_joined = df_units.join(df_pp_filtered, ["CRASH_ID", "UNIT_NBR"], "inner").select(df_units.CRASH_ID, df_units.UNIT_NBR, df_pp_filtered.PRSN_NBR, "PRSN_TYPE_ID", "VEH_MAKE_ID", "PRSN_INJRY_SEV_ID", "PRSN_AIRBAG_ID")
# df_joined_group = df_joined.groupBy("VEH_MAKE_ID").count().orderBy("count", ascending = False).limit(5)
#
# df_joined_group.show()

#4
# df_units_filtered = df_units.filter("VEH_HNR_FL == 'Y'")
# df_pp_filtered = df_pp.filter("(DRVR_LIC_TYPE_ID == 'COMMERCIAL DRIVER LIC.' or DRVR_LIC_TYPE_ID == 'DRIVER LICENSE') and (PRSN_TYPE_ID == 'DRIVER' or PRSN_TYPE_ID == 'DRIVER OF MOTORCYCLE TYPE VEHICLE')")
# # df_pp_filtered.filter("PRSN_TYPE_ID == 'UNKNOWN'").show()
# print(df_units_filtered.count())
# print(df_pp_filtered.count())
# # df_units_filtered.select("UNIT_DESC_ID").distinct().show()
# #
# df_joined = df_units_filtered.join(df_pp_filtered, ["CRASH_ID", "UNIT_NBR"], "inner")
# # df_joined.select("PRSN_TYPE_ID").distinct().show(truncate=False)
# # df_joined.select("DRVR_LIC_TYPE_ID").distinct().show(truncate=False)
# print(df_joined.count())


#5
# df_units.show()
# df_pp.filter("PRSN_GNDR_ID != 'FEMALE'").show()
# df_pp.groupby("")
# df_units.select("VEH_LIC_STATE_ID").distinct().show(100)
#
# # df_pp.select("PRSN_GNDR_ID").distinct().show()
# df_pp_filtered_5 = df_pp.filter("PRSN_GNDR_ID = 'FEMALE'").select("crash_id").distinct()
# df_units_filtered_5 = df_units.select("crash_id", "VEH_LIC_STATE_ID")
# df_joined_5 = df_units_filtered_5.join(df_pp_filtered_5, df_units_filtered_5['crash_id'] == df_pp_filtered_5["crash_id"], "leftanti").select(df_units_filtered_5["crash_id"], "VEH_LIC_STATE_ID")
# df_res = df_joined_5.groupby("VEH_LIC_STATE_ID").count().orderBy("count", ascending = False).limit(1)
# df_res.show()



#6

# df_pp_filtered_6 = df_pp.filter("PRSN_INJRY_SEV_ID != 'NOT INJURED' and PRSN_INJRY_SEV_ID != 'NA' and PRSN_INJRY_SEV_ID != 'UNKNOWN'")
# #
# df_joined = df_units.join(df_pp_filtered_6, ["CRASH_ID", "UNIT_NBR"], "inner")
# #
# df_res = df_joined.groupby("VEH_MAKE_ID").count().orderBy("count", ascending = False)
# # df_res.show()
# windowSpec  = Window.orderBy(col("count").desc())
# df_res_final = df_res.withColumn("rn", row_number().over(windowSpec)).where("rn>=3 and rn<=5").drop("rn")
# df_res_final.show()

#7

# df_joined_7 = df_units.join(df_pp, ["CRASH_ID", "UNIT_NBR"], "inner")
# # print(df_joined_7.count())
# df_joined_7 = df_joined_7.groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count().orderBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
# # df_joined_7.show(100)
# windowSpec  = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
# df_final_7 = df_joined_7.withColumn("rn", row_number().over(windowSpec)).where("rn = 1").drop("rn")
# df_final_7.show()


#8

# df_pp.show()
# df_units.show()
# # df_units.select("CONTRIB_FACTR_1_ID").distinct().show(100, truncate=False)
# # df_units.select("CONTRIB_FACTR_2_ID").distinct().show(100, truncate=False)
df_units_filtered_8 = df_units.filter("CONTRIB_FACTR_1_ID == 'HAD BEEN DRINKING' or CONTRIB_FACTR_2_ID = 'HAD BEEN DRINKING' or CONTRIB_FACTR_P1_ID = 'HAD BEEN DRINKING'")
# print(df_pp.count())
# print(df_units_filtered_8.count())
df_joined_8 = df_units_filtered_8.join(df_pp, ["CRASH_ID", "UNIT_NBR"], "inner")
print(df_joined_8.count())
df_final_8 = df_joined_8.groupby("DRVR_ZIP").count().orderBy("count", ascending = False).filter("DRVR_ZIP is not null").limit(5)
df_final_8.show()

#9

# df_units.select("FIN_RESP_TYPE_ID").distinct().show(100, truncate=False)
# df_units.select("FIN_RESP_PROOF_ID").distinct().show(100, truncate=False)
# df_units.select("FIN_RESP_PROOF_ID", "FIN_RESP_TYPE_ID").distinct().show(100, truncate=False)
# # df_damages.show()
# print(df_damages.count())
# df_damages.select('DAMAGED_PROPERTY').distinct().show(100)
#
# df_units_filtered_9 = df_units.filter("VEH_DMAG_SCL_1_ID like 'DAMAGED%' or VEH_DMAG_SCL_2_ID like 'DAMAGED%'")
# df_units_filtered_9 = df_units_filtered_9.withColumn("damage_1", split("VEH_DMAG_SCL_1_ID", ' ').getItem(1)).withColumn("damage_2", split("VEH_DMAG_SCL_2_ID", ' ').getItem(1))
# df_units_filtered_9 = df_units_filtered_9.filter("cast(damage_1 as int)>4 or cast(damage_2 as int)>4")
# df_units_filtered_9 = df_units_filtered_9.filter("FIN_RESP_TYPE_ID like '%INSURANCE%'")
#
# df_units_filtered_9.select("VEH_DMAG_SCL_1_ID", "damage_1", "VEH_DMAG_SCL_2_ID", "damage_2").distinct().show(100, truncate=False)
# # df_units_filtered_9.select("VEH_DMAG_SCL_2_ID", "damage_2").distinct().show(100, truncate=False)
# print(df_units_filtered_9.count())
# df_final = df_units_filtered_9.join(df_damages, ["CRASH_ID"], "leftanti")
# print(df_final.count())
# print(df_final.select("CRASH_ID").distinct().count())


#10















