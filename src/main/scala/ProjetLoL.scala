import org.apache.spark.sql
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, concat, count, desc, round, sum}


object ProjetLoL {
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


  def countChampion() {
    println("============Top 10 des champions les plus utilisés par les joueurs============")

    participants
      .join(champs, participants("championid") === champs("id"))
      .groupBy("name").count().sort(desc("count"))
      .toDF()
      .show(10, truncate = false)
  }

  def teamsWins() {
    println("============Nombre de wins par équipe============")

    teamstats
      .filter(teamstats("firstblood") === 1)
      .groupBy("teamid").count().sort("count")
      .show(truncate = false)
  }

  def championsWins() {
    println("============Top 10 des champions ayant le plus de win============")

    participants
      .join(champs, participants("championid") === champs("id"))
      .join(stats, participants("id") === stats("id"))
      .filter((participants("player") < 6 and stats("win") === 0) or (participants("player") > 5 and stats("win") === 1))
      .groupBy("name").count().sort(desc("count"))
      .show(10, truncate = false)
  }

  def averageGameTime() {
    println("============Moyenne du temps de jeu d'une partie============")
    matches.select(round(avg("duration") / 60, 2)).show()
  }

  def playerMap() {
    println("============Répartition des joueurs sur la map============")
    participants
      .groupBy("position").count().sort(desc("count"))
      .toDF()
      .withColumn("pourcentage", round((col("count") / participants.count()) * 100))
      .show(truncate = false)
  }

  def banChampion() {
    println("============top 10 Ban champion============")
    teambans
      .join(champs, teambans("championid") === champs("id"))
      .groupBy("name").count().sort(desc("count"))
      .toDF()
      .withColumn("pourcentage de ban global", round((col("count") / teambans.count()) * 100))
      .withColumn("pourcentage de ban par game", round((col("count") / matches.count()) * 100))
      .show(10, truncate = false)
  }

  def killPlayer() {
    println("============Moyenne des kills par joueur============")
    stats
      .select(round(avg("kills"), 2))
      .withColumnRenamed("round(avg(kills), 2)", "Moyenne de kill par joueur")
      .show(truncate = false)
  }

  def killChampion() {
    println("============Top 10 des kills champions ============")
    stats
      .join(participants, participants("id") === stats("id"))
      .join(champs, participants("championid") === champs("id"))
      .toDF()
      .groupBy("name").agg(sum("kills"), sum("quadrakills"), sum("pentakills"), count("name")).sort(desc("sum(kills)"))
      .withColumn("Moyenne de kill par partie", round((col("sum(kills)") / col("count(name)"))))
      .withColumnRenamed("sum(quadrakills)", "Nombre de quadrakills")
      .withColumnRenamed("sum(pentakills)", "Nombre de pentakills")
      .withColumnRenamed("sum(kills)", "Nombre de kill")
      .select("name", "Nombre de kill", "Nombre de quadrakills", "Nombre de pentakills", "Moyenne de kill par partie")
      .show(10, truncate = false)
  }


  def main(args: Array[String]): Unit = {
    //println("hello")
    //print(teamsWins())



  }
}


