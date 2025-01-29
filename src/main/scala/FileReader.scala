import org.apache.spark.sql.SparkSession

object FileReader{
    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
                .appName("FileReader")
                .master("local[*]")
                .getOrCreate()
        
        val data_csv = spark.read.option("header","true").csv("src/main/resources/simple-zipcodes.csv")

        val data_json = spark.read.json("src/main/resources/zipcodes.json")

        val data_txt = spark.read.text("src/main/resources/test.txt")

        val data_parquet = spark.read.parquet("src/main/resources/loan-risks.snappy.parquet")

        println("CSV File Content")
        data_csv.show(5)
        println("CSV File Schema")
        data_csv.printSchema()

        println("JSON File Content")
        data_json.show(5)
        println("JSON File Schema")
        data_json.printSchema()

        println("TXT File Content")
        data_txt.show(5)
        println("TXT File Schema")
        data_txt.printSchema()

        println("Parquet File Content")
        data_parquet.show(5)
        println("Parquet File Schema")
        data_parquet.printSchema()


    }
}