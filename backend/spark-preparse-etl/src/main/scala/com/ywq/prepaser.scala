package com.ywq

import com.ywq.preparser.{PreParsedLog, WebLogPreParser}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}

object prepaser {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
        if (args.isEmpty){
            conf.setMaster("local")
        }
        val spark: SparkSession = SparkSession.builder()
                .appName("PreparseETL")
                .enableHiveSupport()
                .config(conf)
                .getOrCreate()
        val rawdataInputPath: String = spark.conf.get("spark.traffic.analysis.rawdata.input",
            "hdfs://hadoop202:9000/user/yuewenqing/traffic-analysis/rawlog/20180616")
        val numberPartitions: Int = spark.conf.get("spark.traffic.analysis.rawdata.numberPartitions","2").toInt

        val preParsedLogRDD: RDD[PreParsedLog] = spark.sparkContext.textFile(rawdataInputPath)
                .flatMap(line => Option(WebLogPreParser.parse(line)))

        val preParsedLogDS: Dataset[PreParsedLog] = spark.createDataset(preParsedLogRDD)(Encoders.bean(classOf[PreParsedLog]))

        preParsedLogDS.coalesce(numberPartitions)
                .write
                .mode(SaveMode.Append)
                .partitionBy("year","month","day")
                .saveAsTable("rawdata.web")

        spark.stop()
    }
}
