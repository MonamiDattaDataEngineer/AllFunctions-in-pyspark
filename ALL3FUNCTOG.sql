
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import date_sub
from pyspark.sql import functions as F
from datetime import datetime as dt
import argparse
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import when
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from datetime import date, datetime, timedelta
from pyspark.sql.functions import col
from pyspark.sql.functions import date_sub
from pyspark.sql.functions import current_timestamp

  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", True)
spark.conf.set("spark.sql.adaptive.enabled",True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",True)
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
spark.conf.set("spark.sql.broadcastTimeout","6000")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead","CORRECTED")


am_dealer_loc_DF = spark.read.parquet("s3://msil-aos-raw/qlik-data/MULDMS.AM_DEALER_LOC/")
am_dealer_loc_DF = am_dealer_loc_DF.toDF(*[x.upper() for x in am_dealer_loc_DF.columns])

dmsd_ew_fin_DF = spark.read.parquet("s3://msil-aos-raw/qlik-data/MULDMS.DMSD_EW_FIN/")
dmsd_ew_fin_DF = dmsd_ew_fin_DF.toDF(*[x.upper() for x in dmsd_ew_fin_DF.columns])

gd_loyalty_trans_DF = spark.read.parquet("s3://msil-aos-raw/qlik-data/MULDMS.GD_LOYALTY_TRANS_EXPIRY_DATE/")
gd_loyalty_trans_DF = gd_loyalty_trans_DF.toDF(*[x.upper() for x in gd_loyalty_trans_DF.columns])

gd_loyalty_enrol_DF = spark.read.parquet("s3://msil-aos-raw/qlik-data/MULDMS.GD_LOYALTY_ENROL/")
gd_loyalty_enrol_DF = gd_loyalty_enrol_DF.toDF(*[x.upper() for x in gd_loyalty_enrol_DF.columns])

am_list_range_DF = spark.read.parquet("s3://msil-aos-raw/qlik-data/MULDMS.AM_LIST_RANGE/")
am_list_range_DF = am_list_range_DF.toDF(*[x.upper() for x in am_list_range_DF.columns])


# df_AM_DEALER_LOC.createOrReplaceTempView("AM_DEALER_LOC")
# df_dmsd_ew_fin.createOrReplaceTempView("dmsd_ew_fin") 
# df_gd_loyalty_trans.createOrReplaceTempView("gd_loyalty_trans")  
# DF_gd_loyalty_enrol.createOrReplaceTempView("gd_loyalty_enrol") 
# DF_AM_LIST_RANGE.createOrReplaceTempView("AM_LIST_RANGE") 
for_cd_DF1 = am_dealer_loc_DF.select('MUL_DEALER_CD','FOR_CD','OUTLET_CD',col('PARENT_GROUP').alias('LV_PARENT_GROUP'),'DEALER_CATEGORY').distinct()

#outlet_cd_DF1 = am_dealer_loc_DF.select('MUL_DEALER_CD','FOR_CD','OUTLET_CD',col('PARENT_GROUP').alias('LV_PARENT_GROUP'),'DEALER_CATEGORY').distinct()
#delr_cd_DF1 = am_dealer_loc_DF.select('MUL_DEALER_CD','FOR_CD','OUTLET_CD',col('PARENT_GROUP').alias('LV_PARENT_GROUP'),'DEALER_CATEGORY').distinct()
for_cd_DF2 = for_cd_DF1.withColumn('LV_FOR_CD', when(col('DEALER_CATEGORY') == 'MAS', col("FOR_CD")))
for_cd_DF2 = for_cd_DF2.withColumn('LV_DLR_CD', when(col('DEALER_CATEGORY') == 'MAS', concat(lit('MASS'),col('MUL_DEALER_CD'))))
for_cd_DF2 =for_cd_DF2.withColumn('LV_OUTLET_CD', when(col('DEALER_CATEGORY') == 'MAS', col("OUTLET_CD")))

for_cd_DF2.filter(col('DEALER_CATEGORY') == 'MAS').show(5)
for_cd_DF3 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'VCF').select('DEALER_CD','FOR_CD','OUTLET_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
#for_cd_DF3 =  dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'VCF').select('DEALER_CD','FOR_CD','OUTLET_CD',concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD')).distinct()
#for_cd_DF3 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'VCF').select('DEALER_CD','FOR_CD','OUTLET_CD',col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF3.show(5)
for_cd_DF4 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DDL').select('DEALER_CD','FOR_CD','OUTLET_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
#for_cd_DF4 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DDL').select('DEALER_CD','FOR_CD','OUTLET_CD',concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD')).distinct()
#for_cd_DF4 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DDL').select('DEALER_CD','FOR_CD','OUTLET_CD',col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF4.show(5)

for_cd_DF5 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DMM').select('DEALER_CD','FOR_CD','OUTLET_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
#delr_cd_DF5 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DMM').select('DEALER_CD','FOR_CD','OUTLET_CD',concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD')).distinct()
#outlet_cd_DF5 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DMM').select('DEALER_CD','FOR_CD','OUTLET_CD',col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF5.show(5)



for_cd_DF6 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'MUL').select('DEALER_CD','FOR_CD','OUTLET_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
#delr_cd_DF6 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'MUL').select('DEALER_CD','FOR_CD','OUTLET_CD',concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD')).distinct()
#outlet_cd_DF6 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'MUL').select('DEALER_CD','FOR_CD','OUTLET_CD',col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF6.show(5)
# delr_cd_DF2 = am_dealer_loc_DF.filter(col('DEALER_CATEGORY') == 'MAS').select(col('MUL_DEALER_CD').alias('DEALER_CD'),'FOR_CD','OUTLET_CD', concat(lit('MASS'),col('MUL_DEALER_CD')).alias('LV_DLR_CD')).distinct()
for_cd_DF7 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'VCF').select('DEALER_CD','FOR_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
#delr_cd_DF7 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'VCF').select('DEALER_CD','FOR_CD',concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD')).distinct()
#outlet_cd_DF7 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'VCF').select('DEALER_CD','FOR_CD',col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF7.show(5)
for_cd_DF8 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DDL').select('DEALER_CD','FOR_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
#delr_cd_DF8 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DDL').select('DEALER_CD','FOR_CD',concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD')).distinct()
#outlet_cd_DF8 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DDL').select('DEALER_CD','FOR_CD',col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF8.show(5)
for_cd_DF9 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DMM').select('DEALER_CD','FOR_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
#delr_cd_DF9 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DMM').select('DEALER_CD','FOR_CD',concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD')).distinct()
#outlet_cd_DF9 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DMM').select('DEALER_CD','FOR_CD',col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF9.show(5)

for_cd_DF10 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'MUL').select('DEALER_CD','FOR_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
#delr_cd_DF10 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'MUL').select('DEALER_CD','FOR_CD',concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD')).distinct()
#outlet_cd_DF10 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'MUL').select('DEALER_CD','FOR_CD',col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF10.show()

for_cd_DF11 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'VCF').select('DEALER_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
#delr_cd_DF11 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'VCF').select('DEALER_CD',concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD')).distinct()
#outlet_cd_DF11 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'VCF').select('DEALER_CD',col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF11.show(5)
for_cd_DF12 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DDL').select('DEALER_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
#delr_cd_DF12 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DDL').select('DEALER_CD',concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD')).distinct()
#outlet_cd_DF12 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DDL').select('DEALER_CD',col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF12.show(5)

for_cd_DF13 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DMM').select('DEALER_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
#delr_cd_DF13 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DMM').select('DEALER_CD',concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD')).distinct()
#outlet_cd_DF13 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'DMM').select('DEALER_CD',col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF13.show(5)

for_cd_DF14 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'MUL').select('DEALER_CD',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
#delr_cd_DF14 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'MUL').select('DEALER_CD',concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD')).distinct()
#outlet_cd_DF14 = dmsd_ew_fin_DF.filter(col('FINANCIER_DELR_CATG') == 'MUL').select('DEALER_CD',col('FINANCIER_OUTLET_CD').alias('LV_OUTLET_CD')).distinct()
for_cd_DF14.show(5)
interim_join_1 = for_cd_DF1.join(dmsd_ew_fin_DF, (dmsd_ew_fin_DF.DEALER_CD == for_cd_DF1.MUL_DEALER_CD) 
                                    & (dmsd_ew_fin_DF.FOR_CD == for_cd_DF1.FOR_CD) 
                                    & (dmsd_ew_fin_DF.OUTLET_CD == for_cd_DF1.OUTLET_CD) 
                                    ,'inner')\
                            .where(col('LV_PARENT_GROUP').isNotNull() 
                                  & col('FINANCIER_DELR_CATG').isin(['VCF','DDL','DMM','MUL']))\
                            .select('DEALER_CD',dmsd_ew_fin_DF.FOR_CD,dmsd_ew_fin_DF.OUTLET_CD,'FINANCIER_DELR_CATG','LV_PARENT_GROUP',col('FINANCIER_FOR_CD').alias('LV_FOR_CD'),concat(col('FINANCIER_DELR_CATG'),col('FINANCIER_DLR_CD')).alias('LV_DLR_CD'),col('FINANCIER_outlet_CD').alias('LV_OUTLET_CD') )\
                            .distinct()
#Finding Null for lv_for_cd for df2 and df3

#naming for_cd as fortemp for df3 
for_cd_DF = for_cd_DF2.join(for_cd_DF3,((for_cd_DF2.MUL_DEALER_CD == for_cd_DF3.DEALER_CD ) 
                              & (for_cd_DF2.FOR_CD == for_cd_DF3.FOR_CD ) 
                              & (for_cd_DF2.OUTLET_CD == for_cd_DF3.OUTLET_CD ) )
                              ,'left')\
                        .select(for_cd_DF2.MUL_DEALER_CD, for_cd_DF2.FOR_CD , for_cd_DF2.OUTLET_CD, for_cd_DF2.DEALER_CATEGORY,for_cd_DF2.LV_PARENT_GROUP, for_cd_DF2.LV_FOR_CD, for_cd_DF3.LV_FOR_CD.alias('fortemp'),for_cd_DF2.LV_DLR_CD,for_cd_DF3.LV_DLR_CD.alias('temp'), for_cd_DF2.LV_OUTLET_CD, for_cd_DF3.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if df2_forrd is null then df3_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
                       .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                        .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
#Finding Null for lv_for_cd for for_cd_DF and df4

#naming for_cd as fortemp for df4
for_cd_DF = for_cd_DF.join(for_cd_DF4,((for_cd_DF.MUL_DEALER_CD == for_cd_DF4.DEALER_CD ) 
                              & (for_cd_DF.FOR_CD == for_cd_DF4.FOR_CD ) 
                              & (for_cd_DF.OUTLET_CD == for_cd_DF4.OUTLET_CD ) )
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD,for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD, for_cd_DF4.LV_FOR_CD.alias('fortemp'),for_cd_DF4.LV_DLR_CD.alias('temp'),for_cd_DF4.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if for_cd for for_cd_DF is null then df4_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
 .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                        .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
#Finding Null for lv_for_cd for for_cd_DF and df5

#naming for_cd as fortemp for df5
for_cd_DF = for_cd_DF.join(for_cd_DF5,((for_cd_DF.MUL_DEALER_CD == for_cd_DF5.DEALER_CD ) 
                              & (for_cd_DF.FOR_CD == for_cd_DF5.FOR_CD ) 
                              & (for_cd_DF.OUTLET_CD == for_cd_DF5.OUTLET_CD ) )
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD,for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD, for_cd_DF5.LV_FOR_CD.alias('fortemp'),for_cd_DF5.LV_DLR_CD.alias('temp'),for_cd_DF5.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if for_cd for for_cd_DF is null then df5_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
 .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                        .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
#Finding Null for lv_for_cd for for_cd_DF and df6

#naming for_cd as fortemp for df5
for_cd_DF = for_cd_DF.join(for_cd_DF6,((for_cd_DF.MUL_DEALER_CD == for_cd_DF6.DEALER_CD ) 
                              & (for_cd_DF.FOR_CD == for_cd_DF6.FOR_CD ) 
                              & (for_cd_DF.OUTLET_CD == for_cd_DF6.OUTLET_CD ) )
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD,for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD, for_cd_DF6.LV_FOR_CD.alias('fortemp'),for_cd_DF6.LV_DLR_CD.alias('temp'),for_cd_DF6.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if for_cd for for_cd_DF is null then df6_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
 .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                        .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
#Finding Null for lv_for_cd for for_cd_DF and df7

#naming for_cd as fortemp for df7
for_cd_DF = for_cd_DF.join(for_cd_DF7,((for_cd_DF.MUL_DEALER_CD == for_cd_DF7.DEALER_CD ) 
                              & (for_cd_DF.FOR_CD == for_cd_DF7.FOR_CD ) ) 
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD,for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD, for_cd_DF7.LV_FOR_CD.alias('fortemp'),for_cd_DF7.LV_DLR_CD.alias('temp'),for_cd_DF7.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if for_cd for for_cd_DF is null then df7_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
 .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                        .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
#Finding Null for lv_for_cd for for_cd_DF and df8

#naming for_cd as fortemp for df8
for_cd_DF = for_cd_DF.join(for_cd_DF8,((for_cd_DF.MUL_DEALER_CD == for_cd_DF8.DEALER_CD ) 
                              & (for_cd_DF.FOR_CD == for_cd_DF8.FOR_CD ) ) 
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD,for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD, for_cd_DF8.LV_FOR_CD.alias('fortemp'),for_cd_DF8.LV_DLR_CD.alias('temp'),for_cd_DF8.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if for_cd for for_cd_DF is null then df8_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
 .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                        .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
#Finding Null for lv_for_cd for for_cd_DF and df9

#naming for_cd as fortemp for df9
for_cd_DF = for_cd_DF.join(for_cd_DF9,((for_cd_DF.MUL_DEALER_CD == for_cd_DF9.DEALER_CD ) 
                              & (for_cd_DF.FOR_CD == for_cd_DF9.FOR_CD ) ) 
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD,for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD, for_cd_DF9.LV_FOR_CD.alias('fortemp'),for_cd_DF9.LV_DLR_CD.alias('temp'),for_cd_DF9.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if for_cd for for_cd_DF is null then df9_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
 .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                        .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
#Finding Null for lv_for_cd for for_cd_DF and df10

#naming for_cd as fortemp for df10
for_cd_DF = for_cd_DF.join(for_cd_DF10,((for_cd_DF.MUL_DEALER_CD == for_cd_DF10.DEALER_CD ) 
                              & (for_cd_DF.FOR_CD == for_cd_DF10.FOR_CD ) ) 
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD,for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD, for_cd_DF10.LV_FOR_CD.alias('fortemp'),for_cd_DF10.LV_DLR_CD.alias('temp'),for_cd_DF10.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if for_cd for for_cd_DF is null then df10_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
 .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                        .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
#Finding Null for lv_for_cd for for_cd_DF and df11

#naming for_cd as fortemp for df11
for_cd_DF = for_cd_DF.join(for_cd_DF11,((for_cd_DF.MUL_DEALER_CD == for_cd_DF11.DEALER_CD )) 
                             
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD,for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD, for_cd_DF11.LV_FOR_CD.alias('fortemp'),for_cd_DF11.LV_DLR_CD.alias('temp'),for_cd_DF11.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if for_cd for for_cd_DF is null then df11_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
 .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                        .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
#Finding Null for lv_for_cd for for_cd_DF and df12

#naming for_cd as fortemp for df12
for_cd_DF = for_cd_DF.join(for_cd_DF12,((for_cd_DF.MUL_DEALER_CD == for_cd_DF12.DEALER_CD )) 
                             
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD,for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD, for_cd_DF12.LV_FOR_CD.alias('fortemp'),for_cd_DF12.LV_DLR_CD.alias('temp'),for_cd_DF12.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if for_cd for for_cd_DF is null then df12_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
 .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                        .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
#Finding Null for lv_for_cd for for_cd_DF and df13

#naming for_cd as fortemp for df13
for_cd_DF = for_cd_DF.join(for_cd_DF13,((for_cd_DF.MUL_DEALER_CD == for_cd_DF13.DEALER_CD )) 
                             
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD,for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD, for_cd_DF13.LV_FOR_CD.alias('fortemp'),for_cd_DF13.LV_DLR_CD.alias('temp'),for_cd_DF13.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if for_cd for for_cd_DF is null then df13_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
 .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                        .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
#Finding Null for lv_for_cd for for_cd_DF and df14

#naming for_cd as fortemp for df14
for_cd_DF = for_cd_DF.join(for_cd_DF14,((for_cd_DF.MUL_DEALER_CD == for_cd_DF14.DEALER_CD )) 
                             
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD,for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD, for_cd_DF14.LV_FOR_CD.alias('fortemp'),for_cd_DF14.LV_DLR_CD.alias('temp'),for_cd_DF14.LV_OUTLET_CD.alias('outlettemp')).distinct()

#checking if for_cd for for_cd_DF is null then df14_fortemp
for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
 .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                        .withColumn('LV_OUTLET_CD', when(col('LV_OUTLET_CD').isNull(), col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')

for_cd_DF.show(5)
interim_join_1.show(5)
for_cd_DF15 = interim_join_1.filter(col('FINANCIER_DELR_CATG') == 'VCF')
for_cd_DF15.show(5)

for_cd_DF16 = interim_join_1.filter(col('FINANCIER_DELR_CATG') == 'DDL')
for_cd_DF16.show(5)

for_cd_DF17 = interim_join_1.filter(col('FINANCIER_DELR_CATG') == 'DMM')
for_cd_DF17.show(5)


for_cd_DF18 = interim_join_1.filter(col('FINANCIER_DELR_CATG') == 'MUL')
for_cd_DF18.show(5)

##naming for_cd as fortemp for df15
for_cd_DF = for_cd_DF.join(for_cd_DF15,((for_cd_DF.MUL_DEALER_CD == for_cd_DF15.DEALER_CD ) 
                              & (for_cd_DF.FOR_CD == for_cd_DF15.FOR_CD ) 
                              & (for_cd_DF.OUTLET_CD == for_cd_DF15.OUTLET_CD ) 
                               & (for_cd_DF.LV_PARENT_GROUP == for_cd_DF15.LV_PARENT_GROUP ))
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD, for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD,for_cd_DF15.LV_FOR_CD.alias('fortemp'),for_cd_DF15.LV_DLR_CD.alias('temp'),for_cd_DF15.LV_OUTLET_CD.alias('outlettemp')).distinct()


##checking if for_cd for for_cd_DF is null then df15_fortemp

for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
                       .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                       .withColumn('LV_OUTLET_CD' ,when(col('LV_OUTLET_CD').isNull(),col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
##naming for_cd as fortemp for df16
for_cd_DF = for_cd_DF.join(for_cd_DF16,((for_cd_DF.MUL_DEALER_CD == for_cd_DF16.DEALER_CD ) 
                              & (for_cd_DF.FOR_CD == for_cd_DF16.FOR_CD ) 
                              & (for_cd_DF.OUTLET_CD == for_cd_DF16.OUTLET_CD ) 
                               & (for_cd_DF.LV_PARENT_GROUP == for_cd_DF16.LV_PARENT_GROUP ))
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD, for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD,for_cd_DF16.LV_FOR_CD.alias('fortemp'),for_cd_DF16.LV_DLR_CD.alias('temp'),for_cd_DF16.LV_OUTLET_CD.alias('outlettemp')).distinct()


##checking if for_cd for for_cd_DF is null then df16_fortemp

for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
                       .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                       .withColumn('LV_OUTLET_CD' ,when(col('LV_OUTLET_CD').isNull(),col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
##naming for_cd as fortemp for df17
for_cd_DF = for_cd_DF.join(for_cd_DF17,((for_cd_DF.MUL_DEALER_CD == for_cd_DF17.DEALER_CD ) 
                              & (for_cd_DF.FOR_CD == for_cd_DF17.FOR_CD ) 
                              & (for_cd_DF.OUTLET_CD == for_cd_DF17.OUTLET_CD ) 
                               & (for_cd_DF.LV_PARENT_GROUP == for_cd_DF17.LV_PARENT_GROUP ))
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD, for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD,for_cd_DF17.LV_FOR_CD.alias('fortemp'),for_cd_DF17.LV_DLR_CD.alias('temp'),for_cd_DF17.LV_OUTLET_CD.alias('outlettemp')).distinct()


##checking if for_cd for for_cd_DF is null then df17_fortemp

for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
                       .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                       .withColumn('LV_OUTLET_CD' ,when(col('LV_OUTLET_CD').isNull(),col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')

##naming for_cd as fortemp for df18
for_cd_DF = for_cd_DF.join(for_cd_DF18,((for_cd_DF.MUL_DEALER_CD == for_cd_DF18.DEALER_CD ) 
                              & (for_cd_DF.FOR_CD == for_cd_DF18.FOR_CD ) 
                              & (for_cd_DF.OUTLET_CD == for_cd_DF18.OUTLET_CD ) 
                               & (for_cd_DF.LV_PARENT_GROUP == for_cd_DF18.LV_PARENT_GROUP ))
                              ,'left')\
                        .select(for_cd_DF.MUL_DEALER_CD, for_cd_DF.FOR_CD , for_cd_DF.OUTLET_CD, for_cd_DF.DEALER_CATEGORY,for_cd_DF.LV_PARENT_GROUP, for_cd_DF.LV_FOR_CD, for_cd_DF.LV_DLR_CD,for_cd_DF.LV_OUTLET_CD,for_cd_DF18.LV_FOR_CD.alias('fortemp'),for_cd_DF18.LV_DLR_CD.alias('temp'),for_cd_DF18.LV_OUTLET_CD.alias('outlettemp')).distinct()


##checking if for_cd for for_cd_DF is null then df18_fortemp

for_cd_DF = for_cd_DF.withColumn('LV_FOR_CD', when(col('LV_FOR_CD').isNull(), col('fortemp')).otherwise(col('LV_FOR_CD')))\
                       .withColumn('LV_DLR_CD', when(col('LV_DLR_CD').isNull(), col('temp')).otherwise(col('LV_DLR_CD')))\
                       .withColumn('LV_OUTLET_CD' ,when(col('LV_OUTLET_CD').isNull(),col('outlettemp')).otherwise(col('LV_OUTLET_CD')))\
                        .select('MUL_DEALER_CD', 'FOR_CD', 'OUTLET_CD', 'DEALER_CATEGORY', 'LV_PARENT_GROUP', 'LV_FOR_CD','LV_DLR_CD','LV_OUTLET_CD')
for_cd_DF.show()
job.commit()