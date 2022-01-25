import org.apache.spark
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, concat, desc, round}
import org.apache.spark.sql.types.StructType

object ProjetLoL extends App {
  val spark = SparkSession.builder()
    .appName( name ="first sparkAPP")
    .master( master = "local[*]")
    .getOrCreate()


  spark.sparkContext.setLogLevel("ERROR")

  //spark.sparkContext.setLogLevel("INFO")
  val champs = spark
    .read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/champs.csv")

  val matches = spark
    .read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/matches.csv")

  val participants = spark
    .read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/participants.csv")

  val stats = spark
    .read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/stats1.csv","src/main/resources/stats2.csv")

  val teambans = spark
    .read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/teambans.csv")

  val teamstats = spark
    .read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/teamstats.csv")


  println("============Top 10 des champions les plus utilisés par les joueurs============")
  val countChamp = participants
    .join(champs,participants("championid")===champs("id"))
    .groupBy("name").count().sort(desc("count"))
    .show(10,truncate = false)

  println("============Nombre de wins par équipe============")
  val firstKill = teamstats
    .filter(teamstats("firstblood") ===1)
    .groupBy("teamid").count().sort("count")
    .show(truncate= false)

  println("============Top 10 des champions ayant le plus de win============")
  val champWins = participants
    .join(champs,participants("championid")===champs("id"))
    .join(stats,participants("id")===stats("id"))
    .filter((participants("player") < 6 and stats("win")===0) or(participants("player") > 5 and stats("win")===1))
    .groupBy("name").count().sort(desc("count"))
    .show(10,truncate = false)

  println("============Moyenne du temps de jeu d'une partie============")
  matches.select(avg("duration")/60).show()

  println("============Répartition des joueurs sur la map============")
  val position = participants
    .groupBy("position").count().sort(desc("count"))
    .toDF()
    .withColumn("pourcentage",round((col("count")/participants.count())*100))
    .show(truncate = false)


}


