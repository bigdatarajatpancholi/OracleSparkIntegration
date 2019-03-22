import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object MainClass {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Oracle Integration with Spark")
      .config("spark.sql.warehouse.dir", "/tmp")
      .getOrCreate()



    val empDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:india/india@//192.168.153.1:1521/xe")
      .option("dbtable", "india.emp")
      .option("user", "india")
      .option("password", "india")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load()
    empDF.printSchema()
    empDF.show(false)

  }
}
