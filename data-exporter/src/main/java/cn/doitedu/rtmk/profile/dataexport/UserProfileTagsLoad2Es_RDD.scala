package cn.doitedu.rtmk.profile.dataexport

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object UserProfileTagsLoad2Es_RDD {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "doitedu")
      .set("es.port", "9200")
      .set("es.nodes.wan.only", "true")

    // 创建spark编程入口
    val spark = SparkSession.builder()
      .config(conf)
      .appName("")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()


    // 读hive的画像标签表
    val df = spark.read.table("test.user_profile_test")
    //val df = spark.read.option("header", "true").option("sep", "_").csv("data/user_profile_test.txt")

    // 整成es所需要的格式 RDD[Map]
    val tagsRdd = df.rdd.map(row => {
      Map("guid" -> row.getAs[Int]("guid"),
        "tg01" -> row.getAs[Int]("tg01"),
        "tg02" -> row.getAs[Int]("tg02"),
        "tg03" -> row.getAs[String]("tg03"),
        "tg04" -> row.getAs[Array[String]]("tg04")
      )
    })

    tagsRdd.foreach(println)

    // 调用api插入数据到es
    import org.elasticsearch.spark._
    tagsRdd.saveToEs("profile_tags2/",Map("es.mapping.id" -> "guid"))

    spark.close()
  }
}
