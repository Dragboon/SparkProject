import org.apache.spark.sql
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, concat, count, desc, round, sum}
import scala.io.StdIn.readInt


object ProjetLoL {

  val spark = SparkSession.builder()
    .appName( name ="first sparkAPP")
    .master( master = "local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")


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

  def showMenu(){
    println()
    println("¸,ø¤º°`°º¤ø,¸¸,ø¤º°`°º¤ø,¸,ø¤°º¤ø,¸¸,ø¤º°`°º¤ø,¸")
    println("1• Les champions les plus utilisés")
    println("2• Nombre de victoire en fonction de l'équipe")
    println("3• Les victoires par champion")
    println("4• La moyenne du temps de jeu par partie")
    println("5• La répartition des joueurs sur la map")
    println("6• Les bans champion")
    println("7• Le nombre de kills par joueur")
    println("8• Le nombre de kills par champion")
    println("9• Sortie")
    println("¸,ø¤º°`°º¤ø,¸¸,ø¤º°`°º¤ø,¸,ø¤°º¤ø,¸¸,ø¤º°`°º¤ø,¸")
    print("Merci d'indiquer l'information à consulter: ")
  }


  def main(args: Array[String]): Unit = {
    var choice = 0

    println("┈╱╱▏┈┈╱╱╱╱▏╱╱▏┈┈┈")
    println("┈▇╱▏┈┈▇▇▇╱▏▇╱▏┈┈┈   Bienvenue dans l'analyse")
    println("┈▇╱▏▁┈▇╱▇╱▏▇╱▏▁┈┈   des parties League Of legends")
    println("┈▇╱╱╱▏▇╱▇╱▏▇╱╱╱▏┈   Choisissez les informations")
    println("┈▇▇▇╱┈▇▇▇╱┈▇▇▇╱┈┈   que vous souhaitez consulter")


    while(choice !=10) {
      showMenu()
      choice = readInt()

      choice match {
        case 1 => countChampion()
        case 2 => teamsWins()
        case 3 => championsWins()
        case 4 => averageGameTime()
        case 5 => playerMap()
        case 6 => banChampion()
        case 7 => killPlayer()
        case 8 => killChampion()
        case 9 => System.exit(0)
        case _ => println("Merci de saisir une valeur existante")
      }
    }
  }

}


