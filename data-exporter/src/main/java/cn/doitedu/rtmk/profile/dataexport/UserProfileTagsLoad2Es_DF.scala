package cn.doitedu.rtmk.profile.dataexport

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

/**
 * @Author: deep as the sea
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2022/9/15
 * @Desc: 学大数据，到多易教育
 *
 * 开发此模块时，遇到了es依赖和spark依赖之间的jar包版本冲突问题
 * 版本冲突的底层逻辑是：
 *    有一个模块 A(spark) 要调用一个依赖 slf4j-1.5版本的某方法 a
 *    有一个模块 B(es) 要调用一个依赖 lf4j-2的某方法 b
 *
 * 而项目中同时引入了   slf4j-1.5  和  slf4j-2.0
 * 而最终在项目中起作用的是  slf4j-2.0 （假设 slf4j-2.0 版本中已经没有a方法了）
 * 所以，模块A的工作时会报错
 *
 * 解决办法：  一般来说，是留下目标依赖的 “新版本” ，因为新版本中一般都保留了老版本中的功能（类、方法）
 *
 * 如何去分析出发生了冲突的 依赖jar包呢？
 *  - 可以在工程目录中，使用maven命令打印出整个工程的依赖树
 *   mvn dependency:tree  >  a.txt
 *
 *  - 也可以利用idea的 maven helper插件，进行可视化分析
 *
 *
 **/
object UserProfileTagsLoad2Es_DF {

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

    EsSparkSQL.saveToEs(df,"profile_tags3",Map("es.mapping.id" -> "guid"))

    spark.close()
  }
}
