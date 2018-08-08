package examples

import org.apache.spark.{SparkConf, SparkContext}

object TestOrder1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("testorder1")
    val sc = new SparkContext(conf)
    val array = Array(
      ("solar",7005771),
      ("hi",7003404),
      ("earth",6995884),
      ("balance",6001768),
      ("more",6002554),
      ("energy",6002590),
      ("planet",7001354),
      ("you",6001736),
      ("guess",5996422),
      ("evict",6000207),
      ("english",5998350),
      ("hadoop",5996052),
      ("less",6000501),
      ("me",5997597),
      ("ass",6000092),
      ("benefit",5995718)
    )
    val rdd1 = sc.parallelize(array)
    val rdd2 = rdd1.map(f => (f._2, f._1))
    val rdd3 = rdd2.sortByKey(false)
    val rdd4 = rdd3.map(f => (f._2, f._1))
    rdd4.foreach{
      f => println(f)
    }


  }
}
