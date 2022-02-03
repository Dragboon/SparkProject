import org.apache.spark.sql
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, concat, count, desc, round, sum}


object ProjetLoL {

  def countChampion() {
    println("============Top 10 des champions les plus utilisés par les joueurs============")

    DataAccess.participants
      .join(DataAccess.champs, DataAccess.participants("championid") === DataAccess.champs("id"))
      .groupBy("name").count().sort(desc("count"))
      .toDF()
      .show(10, truncate = false)
  }

  def teamsWins() {
    println("============Nombre de wins par équipe============")

    DataAccess.teamstats
      .filter(DataAccess.teamstats("firstblood") === 1)
      .groupBy("teamid").count().sort("count")
      .show(truncate = false)
  }

  def championsWins() {
    println("============Top 10 des champions ayant le plus de win============")

    DataAccess.participants
      .join(DataAccess.champs, DataAccess.participants("championid") === DataAccess.champs("id"))
      .join(DataAccess.stats, DataAccess.participants("id") === DataAccess.stats("id"))
      .filter((DataAccess.participants("player") < 6 and DataAccess.stats("win") === 0) or (DataAccess.participants("player") > 5 and DataAccess.stats("win") === 1))
      .groupBy("name").count().sort(desc("count"))
      .show(10, truncate = false)
  }

  def averageGameTime() {
    println("============Moyenne du temps de jeu d'une partie============")
    DataAccess.matches.select(round(avg("duration") / 60, 2)).show()
  }

  def playerMap() {
    println("============Répartition des joueurs sur la map============")
    DataAccess.participants
      .groupBy("position").count().sort(desc("count"))
      .toDF()
      .withColumn("pourcentage", round((col("count") / DataAccess.participants.count()) * 100))
      .show(truncate = false)
  }

  def banChampion() {
    println("============top 10 Ban champion============")
    DataAccess.teambans
      .join(DataAccess.champs, DataAccess.teambans("championid") === DataAccess.champs("id"))
      .groupBy("name").count().sort(desc("count"))
      .toDF()
      .withColumn("pourcentage de ban global", round((col("count") / DataAccess.teambans.count()) * 100))
      .withColumn("pourcentage de ban par game", round((col("count") / DataAccess.matches.count()) * 100))
      .show(10, truncate = false)
  }

  def killPlayer() {
    println("============Moyenne des kills par joueur============")
    DataAccess.stats
      .select(round(avg("kills"), 2))
      .withColumnRenamed("round(avg(kills), 2)", "Moyenne de kill par joueur")
      .show(truncate = false)
  }

  def killChampion() {
    println("============Top 10 des kills champions ============")
    DataAccess.stats
      .join(DataAccess.participants, DataAccess.participants("id") === DataAccess.stats("id"))
      .join(DataAccess.champs, DataAccess.participants("championid") === DataAccess.champs("id"))
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
    println("hello")
    print(teamsWins())



  }
}


