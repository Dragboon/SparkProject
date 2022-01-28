import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, concat, count, desc, round, sum}


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
    .toDF()
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
  matches.select(round(avg("duration")/60,2)).show()

  println("============Répartition des joueurs sur la map============")
  val position = participants
    .groupBy("position").count().sort(desc("count"))
    .toDF()
    .withColumn("pourcentage",round((col("count")/participants.count())*100))
    .show(truncate = false)

  println("============top 10 Ban champion============")
  val banChampion = teambans
    .join(champs,teambans("championid")===champs("id"))
    .groupBy("name").count().sort(desc("count"))
    .toDF()
    .withColumn("pourcentage de ban global",round((col("count")/teambans.count())*100))
    .withColumn("pourcentage de ban par game",round((col("count")/matches.count())*100))
    .show(10,truncate = false)


  println("============Moyenne des kills par joueur============")
  val meanKills = stats
    .select(round(avg("kills"),2))
    .withColumnRenamed("round(avg(kills), 2)","Moyenne de kill par joueur")
    .show(truncate = false)

  println("============Top 10 des kills champions ============")
  val meanKillsChampions = stats
    .join(participants,participants("id")===stats("id"))
    .join(champs,participants("championid")===champs("id"))
    .toDF()
    .groupBy("name").agg(sum("kills"),sum("quadrakills"),sum("pentakills"),count("name")).sort(desc("sum(kills)"))
    .withColumn("Moyenne de kill par partie",round((col("sum(kills)")/col("count(name)"))))
    .withColumnRenamed("sum(quadrakills)","Nombre de quadrakills")
    .withColumnRenamed("sum(pentakills)","Nombre de pentakills")
    .withColumnRenamed("sum(kills)","Nombre de kill")
    .select("name","Nombre de kill","Nombre de quadrakills","Nombre de pentakills","Moyenne de kill par partie")
    .show(10,truncate = false)

}


