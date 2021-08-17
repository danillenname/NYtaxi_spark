package com.tsc.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object PercentagePerPsgSQL {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
            .master("local[4]")
            .appName("SparkByExample")
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        //считывание исходного dataFrame и фильтрация по месяцу
        val df1 = spark.read.option("header", value = true).csv("src/main/resources/csv/yellow_tripdata_2020-01.csv")
            .filter("cast(tpep_pickup_datetime as date) between '2020-01-01' and '2020-01-31' ")
        df1.createOrReplaceTempView("table1")

        //подсчет общего количества поездок за каждый из дней месяца
        val df2 = spark
            .sql("select cast(tpep_pickup_datetime as date) as date, " +
                "count(passenger_count) as total_psg " +
                "from table1 " +
                "group by cast(tpep_pickup_datetime as date) " +
                "order by date")
        df2.createOrReplaceTempView("total_psg")

        //подсчет поездок с 0 пассажиров за каждый из дней, далее аналогично для 1, 2, 3 и 4+
        val df3 = spark
            .sql("select cast(tpep_pickup_datetime as date) as date, " +
                "count(passenger_count) as 0_psg, " +
                "max(cast(Total_amount as double)) as max_price_0p, " +
                "min(cast(Total_amount as double)) as min_price_0p " +
                "from table1 " +
                "where passenger_count = 0 " +
                "group by cast(tpep_pickup_datetime as date)")
        df3.createOrReplaceTempView("0_psg")

        val df4 = spark
            .sql("select cast(tpep_pickup_datetime as date) as date, " +
                "count(passenger_count) as 1_psg, " +
                "max(cast(Total_amount as double)) as max_price_1p, " +
                "min(cast(Total_amount as double)) as min_price_1p " +
                "from table1 " +
                "where passenger_count = 1 " +
                "group by cast(tpep_pickup_datetime as date)")
        df4.createOrReplaceTempView("1_psg")

        val df5 = spark
            .sql("select cast(tpep_pickup_datetime as date) as date, " +
                "count(passenger_count) as 2_psg, " +
                "max(cast(Total_amount as double)) as max_price_2p, " +
                "min(cast(Total_amount as double)) as min_price_2p " +
                "from table1 " +
                "where passenger_count = 2 " +
                "group by cast(tpep_pickup_datetime as date)")
        df5.createOrReplaceTempView("2_psg")

        val df6 = spark
            .sql("select cast(tpep_pickup_datetime as date) as date, " +
                "count(passenger_count) as 3_psg, " +
                "max(cast(Total_amount as double)) as max_price_3p, " +
                "min(cast(Total_amount as double)) as min_price_3p " +
                "from table1 " +
                "where passenger_count = 3 " +
                "group by cast(tpep_pickup_datetime as date)")
        df6.createOrReplaceTempView("3_psg")

        val df7 = spark
            .sql("select cast(tpep_pickup_datetime as date) as date, " +
                "count(passenger_count) as 4plus_psg, " +
                "max(cast(Total_amount as double)) as max_price_4plus_p, " +
                "min(cast(Total_amount as double)) as min_price_4plus_p " +
                "from table1 " +
                "where passenger_count >= 4 " +
                "group by cast(tpep_pickup_datetime as date)")
        df7.createOrReplaceTempView("4plus_psg")

        //соединение полученных таблиц и нахождение процента поездок с разным числов пассажиров
        val df8 = spark
            .sql("select tot.date, " +
                "coalesce(cast(100 * 0_psg / total_psg as numeric(4, 2)), 'N/A') as percentage_zero, " +
                "coalesce(max_price_0p, 'N/A') as max_price_0p, " +
                "coalesce(min_price_0p, 'N/A') as min_price_0p, " +
                "coalesce(cast(100 * 1_psg / total_psg as numeric(4, 2)), 'N/A')  as percentage_1p, " +
                "coalesce(max_price_1p, 'N/A') as max_price_1p, " +
                "coalesce(min_price_1p, 'N/A') as min_price_1p, " +
                "coalesce(cast(100 * 2_psg / total_psg as numeric(4, 2)), 'N/A') as percentage_2p, " +
                "coalesce(max_price_2p, 'N/A') as max_price_2p, " +
                "coalesce(min_price_2p, 'N/A') as min_price_2p, " +
                "coalesce(cast(100 * 3_psg / total_psg as numeric(4, 2)), 'N/A') as percentage_3p, " +
                "coalesce(max_price_3p, 'N/A') as max_price_3p, " +
                "coalesce(min_price_3p, 'N/A') as min_price_3p, " +
                "coalesce(cast(100 * 4plus_psg / total_psg as numeric(4, 2)), 'N/A') as percentage_4p_plus, " +
                "coalesce(max_price_4plus_p, 'N/A') as max_price_4plus_p, " +
                "coalesce(min_price_4plus_p, 'N/A') as min_price_4plus_p " +
                "from total_psg tot " +
                "left join 0_psg on tot.date = 0_psg.date " +
                "left join 1_psg on tot.date = 1_psg.date " +
                "left join 2_psg on tot.date = 2_psg.date " +
                "left join 3_psg on tot.date = 3_psg.date " +
                "left join 4plus_psg on tot.date = 4plus_psg.date " +
                "order by date")

        df8.show(31)

        spark.conf.set("spark.sql.shuffle.partitions", "2")
        df8.repartition(2).write.format("parquet").mode("overwrite").save("tmp/parquet/res")
    }
}
