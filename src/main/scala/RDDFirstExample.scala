import org.apache.spark.sql.SparkSession

object RDDFirstExample {
    def main(args: Array[String]): Unit = { 

        val spark = SparkSession.builder()
                .appName("RDDFirstExample")
                .master("local[*]")
                .getOrCreate()

        val data = Seq(1,2,3,4,5)

        val rdd = spark.sparkContext.parallelize(data)

        println("Normal Print: " + rdd.collect().toList)
        println("Normal Without list: " + rdd.collect()) //This will print random value
        println("Formatted Print: " + rdd.collect().mkString(", "))

        spark.stop()        

    }
}