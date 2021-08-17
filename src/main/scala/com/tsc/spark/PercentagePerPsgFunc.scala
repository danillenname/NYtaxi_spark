package com.tsc.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, max, min}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

object PercentagePerPsgFunc {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .master("local[4]")
            .appName("exmpl")
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        //создание схемы для проставления типов
        val simpleSchema = StructType(Array(
            StructField("VendorID", IntegerType, true),
            StructField("tpep_pickup_datetime", DateType, true),
            StructField("tpep_dropoff_datetime", DateType, true),
            StructField("passenger_count", IntegerType, true),
            StructField("Trip_distance", DoubleType, true),
            StructField("PULocationID", IntegerType, true),
            StructField("DOLocationID", StringType, true),
            StructField("RateCodeID", IntegerType, true),
            StructField("Store_and_fwd_flag", StringType, true),
            StructField("Payment_type", IntegerType, true),
            StructField("Fare_amount", DoubleType, true),
            StructField("Extra", DoubleType, true),
            StructField("MTA_tax", DoubleType, true),
            StructField("Improvement_surcharge", DoubleType, true),
            StructField("Tip_amount", DoubleType, true),
            StructField("Tolls_amount", DoubleType, true),
            StructField("Total_amount", DoubleType, true)
        ))

        //чтение из файла и фильтрация по дате
        val df1 = spark.read
            .option("header", value = true)
            .schema(simpleSchema)
            .csv("src/main/resources/csv/yellow_tripdata_2020-01.csv")
            .filter("tpep_pickup_datetime between '2020-01-01' and '2020-01-31'")

        //формирование датафрейма с общим количеством поездок за каждый день
        val total_psg = df1
            .where(col("passenger_count").isNotNull)
            .groupBy("tpep_pickup_datetime")
            .agg(
                count("*").as("trip_total"))

        //создание датафрейма с количеством поездок за каждый день и минимальной/максимальной стоимостью
        //в случае поездок без пассажиров
        //далее аналогично для 1, 2, 3, 4+ пассажиров
        val zero_psg = df1
            .where(col("passenger_count") === 0)
            .groupBy("tpep_pickup_datetime")
            .agg(
                count("*").as("trip_0p"),
                min("total_amount").as("min_0p"),
                max("total_amount").as("max_0p"))

        val one_psg = df1
            .where(col("passenger_count") === 1)
            .groupBy("tpep_pickup_datetime")
            .agg(
                count("*").as("trip_1p"),
                min("total_amount").as("min_1p"),
                max("total_amount").as("max_1p"))

        val two_psg = df1
            .where(col("passenger_count") === 2)
            .groupBy("tpep_pickup_datetime")
            .agg(
                count("*").as("trip_2p"),
                min("total_amount").as("min_2p"),
                max("total_amount").as("max_2p"))

        val three_psg = df1
            .where(col("passenger_count") === 3)
            .groupBy("tpep_pickup_datetime")
            .agg(
                count("*").as("trip_3p"),
                min("total_amount").as("min_3p"),
                max("total_amount").as("max_3p"))

        val four_plus_psg = df1
            .where(col("passenger_count") >= 4)
            .groupBy("tpep_pickup_datetime")
            .agg(
                count("*").as("trip_4plus_p"),
                min("total_amount").as("min_4p"),
                max("total_amount").as("max_4p"))

        //создание сводного датафрейма с количеством поездок по дням
        val sum_table = {
            total_psg
                .join(zero_psg, Seq("tpep_pickup_datetime"), "inner")
                .join(one_psg, Seq("tpep_pickup_datetime"), "inner")
                .join(two_psg, Seq("tpep_pickup_datetime"), "inner")
                .join(three_psg, Seq("tpep_pickup_datetime"), "inner")
                .join(four_plus_psg, Seq("tpep_pickup_datetime"), "inner")
                .orderBy("tpep_pickup_datetime")
        }

        //расчет процента поездок для каждого количества пассажиров
        val result =
            sum_table.select(
                col("tpep_pickup_datetime").as("date"),
                (sum_table("trip_0p") / sum_table("trip_total") * 100.0).cast("numeric(6, 2)").as("percentage_0"),
                sum_table("min_0p"),
                sum_table("max_0p"),
                (sum_table("trip_1p") / sum_table("trip_total") * 100.0).cast("numeric(6, 2)").as("percentage_1"),
                sum_table("min_1p"),
                sum_table("max_1p"),
                (sum_table("trip_2p") / sum_table("trip_total") * 100.0).cast("numeric(6, 2)").as("percentage_2"),
                sum_table("min_2p"),
                sum_table("max_2p"),
                (sum_table("trip_3p") / sum_table("trip_total") * 100.0).cast("numeric(6, 2)").as("percentage_3"),
                sum_table("min_3p"),
                sum_table("max_3p"),
                (sum_table("trip_4plus_p") / sum_table("trip_total") * 100.0).cast("numeric(6, 2)").as("percentage_4plus"),
                sum_table("min_4p"),
                sum_table("max_4p")
            )

        result.show(31)

        spark.conf.set("spark.sql.shuffle.partitions", "2")
        result.repartition(2).write.format("parquet").mode("overwrite").save("tmp/parquet/resFunc")
    }
}
