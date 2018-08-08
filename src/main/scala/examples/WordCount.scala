package examples

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val startTimeMillis = System.currentTimeMillis()
//    val logFile = "D:/test/hadoop_data/wordcount_data/in/inputFile1.txt"
    val logFile = "hdfs://192.168.1.20:9000/test/wordcount/in/inputFile1.txt"
    //放到集群中运行时不要用setMaster
//    val conf = new SparkConf().setMaster("local[*]").setAppName("wc")
    val conf = new SparkConf().setAppName("wc")
    val sc = new SparkContext(conf)
    val text = sc.textFile(logFile)
    val words = text.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word, 1))
    val results = pairs.reduceByKey(_ + _)
    val results2 = results.map(tuple => (tuple._2, tuple._1))
    val sorted = results2.sortByKey(false).map(tp => (tp._2, tp._1))
    val result = sorted.collect()
    result.foreach { x => println(x) }
    val endTimeMillis = System.currentTimeMillis()
    println("总耗时：" + (endTimeMillis - startTimeMillis))
  }
}
