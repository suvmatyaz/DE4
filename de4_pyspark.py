from __future__ import print_function

from pyspark.sql import SparkSession

from pyspark.sql.context import SQLContext

from kaggle.api.kaggle_api_extended import KaggleApi

import os
import sys

if __name__ == "__main__":

    input_ds = sys.argv[1]
    ds_local_path = sys.argv[2]
    output_path = sys.argv[3]

    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(input_ds, unzip=True, path=ds_local_path)

    spark = SparkSession.builder.appName("DE-4").getOrCreate()

    sqlContext = SQLContext(spark)

    sqlContext.sql("Create table if not exists DE4 (INCIDENT_NUMBER string, OFFENSE_CODE string, OFFENSE_CODE_GROUP string, OFFENSE_DESCRIPTION string, DISTRICT string, REPORTING_AREA integer, SHOOTING string, OCCURRED_ON_DATE Timestamp, YEAR integer, MONTH integer, DAY_OF_WEEK string, HOUR string, UCR_PART string, STREET string, Lat Decimal(8,6), Long Decimal(9,6), Location string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' ")

    query = "LOAD DATA LOCAL INPATH '{}' OVERWRITE INTO TABLE DE4".format(ds_local_path+'/crime.csv')
    sqlContext.sql(query)

    df = sqlContext.sql("""
        with crimes as (
            select * from (
                select INCIDENT_NUMBER,
                    OFFENSE_CODE_GROUP as crime_type,
                    DISTRICT,
                    cast(YEAR as integer),
                    cast(month as integer),
                    cast(Lat as Decimal(8,6)),
                    cast(long as Decimal(9,6))
                from DE4
                where INCIDENT_NUMBER != 'INCIDENT_NUMBER' and DISTRICT != ''
            ) crimes_filt
            where INCIDENT_NUMBER is not null and crime_type is not null and DISTRICT is not null and YEAR is not null and MONTH is not null
                and Lat is not null and Long is not null
            group by INCIDENT_NUMBER, crime_type, DISTRICT, YEAR, MONTH, Lat, Long
        ), frequent_crime_types  as (
            select DISTRICT, crime_types[0] || ', ' || crime_types[1] || ', ' || crime_types[2] as crime_types
            from (
                select DISTRICT , collect_list(crime_type) as crime_types
                from (
                    select DISTRICT, crime_type , count(*) as crimes_total
                    from crimes
                    group by DISTRICT, crime_type
                    order by DISTRICT, count(*) desc
                ) types_sort
                group by DISTRICT
            ) top_types
        ), district_crimes as (
            select DISTRICT, sum(crimes_total) as crimes_total, percentile_approx(crimes_total, 0.5) as crimes_monthly , avg(lat) as lat, avg(lng) as lng
            from (
                select DISTRICT, month||'.'||year as m, count(*) as crimes_total , avg(Lat) as lat, avg(long) as lng
                from crimes
                group by DISTRICT, month, year
            ) crime_by_d
            group by DISTRICT
        )
        select d.*, f.crime_types
        from district_crimes d
        join frequent_crime_types f
        on d.district = f.district
    """)

    df.write.mode('overwrite').parquet(output_path)

    df.drop()

    spark.stop()
