import org.apache.spark.sql.SparkSession

object DataAccess {
  val spark = SparkSession.builder()
    .appName( name ="first sparkAPP")
    .master( master = "local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")



  val champs = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/champs.csv")

  val matches = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/matches.csv")

  val participants = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/participants.csv")

  val stats = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/stats1.csv", "src/main/resources/stats2.csv")

  val teambans = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/teambans.csv")

  val teamstats = spark
    .read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/teamstats.csv")
}
