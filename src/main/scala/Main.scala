import org.apache.spark.sql.SparkSession

object Main {
    def main (args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName("ScalaSparkFirstExample")
            .master("local[*]")
            .getOrCreate()
        
        val data = Seq(("Alice", 29), ("Bob", 31), ("Cathy", 25))
        val df = spark.createDataFrame(data).toDF("Name", "Age")
        df.show()

        spark.stop()
    }
}