package eci.edu.co

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  // Starting point of any application
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]") //In a real cluster the ip of the master node
      .appName("sparkLocalSession")
      .getOrCreate()
  }
}
