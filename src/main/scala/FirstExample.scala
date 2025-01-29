import org.apache.spark.sql.SparkSession

object FirstExample {
    def main (args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName("ScalaSparkFirstExample")
            .master("local[*]")
            .getOrCreate()
        
        val data = Seq(("Alice", 29), ("Bob", 31), ("Cathy", 25))
        val df = spark.createDataFrame(data).toDF("Name", "Age")
        df.show()
        println(s"Spark Version: ${spark.version}")

        spark.stop()
    }
}