import org.apache.spark.sql
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, concat, count, desc, round, sum}
import scala.io.StdIn.readInt
import scala.io.Source


object ProjetLoL {

  val spark = SparkSession.builder()
    .appName( name ="first sparkAPP")
    .master( master = "local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")


  def countChampion() {
    println("============Top champions les plus utilisés par les joueurs============")
    print("Merci d'indiquer le nom de champion à afficher: ")
    var countChamp = readInt()
    DataAccess.participants
      .join(DataAccess.champs, DataAccess.participants("championid") === DataAccess.champs("id"))
      .groupBy("name").count().sort(desc("count"))
      .toDF()
      .withColumnRenamed("name", "Champion")
      .show(countChamp, truncate = false)
  }

  def teamsWins() {
    println("============Nombre de wins par équipe============")
    DataAccess.teamstats
      .filter(DataAccess.teamstats("firstblood") === 1)
      .groupBy("teamid").count().sort("count")
      .withColumnRenamed("teamid", "Team")
      .show(truncate = false)
  }

  def championsWins() {
    println("============Top champions ayant le plus de win============")
    print("Merci d'indiquer le nom de champion à afficher: ")
    var countChamp = readInt()
    DataAccess.participants
      .join(DataAccess.champs, DataAccess.participants("championid") === DataAccess.champs("id"))
      .join(DataAccess.stats, DataAccess.participants("id") === DataAccess.stats("id"))
      .filter((DataAccess.participants("player") < 6 and DataAccess.stats("win") === 0) or (DataAccess.participants("player") > 5 and DataAccess.stats("win") === 1))
      .groupBy("name").count().sort(desc("count"))
      .withColumnRenamed("name", "Champion")
      .show(countChamp, truncate = false)
  }

  def averageGameTime() {
    println("============Moyenne du temps de jeu d'une partie============")
    DataAccess.matches
      .select(round(avg("duration") / 60, 2))
      .withColumnRenamed("round((avg(duration) / 60), 2)", "Moyenne")
      .show()
  }

  def playerMap() {
    println("============Répartition des joueurs sur la map============")
    DataAccess.participants
      .groupBy("position").count().sort(desc("count"))
      .toDF()
      .withColumn("Pourcentage", round((col("count") / DataAccess.participants.count()) * 100))
      .show(truncate = false)
  }

  def banChampion() {
    println("============top Ban champion============")
    print("Merci d'indiquer le nom de champion à afficher: ")
    var countChamp = readInt()
    DataAccess.teambans
      .join(DataAccess.champs, DataAccess.teambans("championid") === DataAccess.champs("id"))
      .groupBy("name").count().sort(desc("count"))
      .toDF()
      .withColumn("Pourcentage de ban global", round((col("count") / DataAccess.teambans.count()) * 100))
      .withColumn("Pourcentage de ban par game", round((col("count") / DataAccess.matches.count()) * 100))
      .withColumnRenamed("name", "Champion")
      .show(countChamp, truncate = false)
  }

  def killPlayer() {
    println("============Moyenne des kills par joueur============")
    DataAccess.stats
      .select(round(avg("kills"), 2))
      .withColumnRenamed("round(avg(kills), 2)", "Moyenne de kill par joueur")
      .show(truncate = false)
  }

  def killChampion() {
    println("============Top des kills champions ============")
    print("Merci d'indiquer le nom de champion à afficher: ")
    var countChamp = readInt()
    DataAccess.stats
      .join(DataAccess.participants, DataAccess.participants("id") === DataAccess.stats("id"))
      .join(DataAccess.champs, DataAccess.participants("championid") === DataAccess.champs("id"))
      .toDF()
      .groupBy("name").agg(sum("kills"), sum("quadrakills"), sum("pentakills"), count("name")).sort(desc("sum(kills)"))
      .withColumn("Moyenne de kill par partie", round((col("sum(kills)") / col("count(name)"))))
      .withColumnRenamed("name", "Champion")
      .withColumnRenamed("sum(quadrakills)", "Nombre de quadrakills")
      .withColumnRenamed("sum(pentakills)", "Nombre de pentakills")
      .withColumnRenamed("sum(kills)", "Nombre de kill")
      .select("name", "Nombre de kill", "Nombre de quadrakills", "Nombre de pentakills", "Moyenne de kill par partie")
      .show(countChamp, truncate = false)
  }


  def main(args: Array[String]): Unit = {
    var choice = 0

    val welcome = "src/main/text/welcome.txt"
    for (line <- Source.fromFile(welcome).getLines) {
      println(line)
    }


    while(choice !=9){
      val menu = "src/main/text/menu.txt"
      for (line <- Source.fromFile(menu).getLines) {
        println(line)
      }
      print("Merci d'indiquer l'information à consulter: ")
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


