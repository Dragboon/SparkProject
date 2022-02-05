import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, desc, round, sum}
import scala.io.StdIn.readInt
import scala.io.Source


object ProjetLoL {

  val spark = SparkSession.builder()
    .appName( name ="first sparkAPP")
    .master( master = "local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")



  def principalInformations() {
    println("============Informations principales============")
    DataAccess.stats
    .join(DataAccess.participants, DataAccess.participants("id") === DataAccess.stats("id"))
    .select(sum("kills"),sum("deaths"),count("player"),countDistinct("matchid"),count("championid"))
    .withColumnRenamed("sum(kills)", "Nombre global de kills")
    .withColumnRenamed("sum(deaths)", "Nombre global de morts")
    .withColumnRenamed("count(player)", "Nombre de joueur")
    .withColumnRenamed("count(DISTINCT matchid)", "Nombre de parties")
    .withColumnRenamed("count(DISTINCT championid)", "Nombre de champions")
    .show(truncate = false)
  }

  def countChampion() {
    println("============Top champions les plus utilisés par les joueurs============")
    print("Merci d'indiquer le nom de champion à afficher: ")
    var countChamp = readInt()
    DataAccess.participants
      .join(DataAccess.champs, DataAccess.participants("championid") === DataAccess.champs("id"))
      .groupBy("name").count().sort(desc("count"))
      .withColumn("Pourcentage d'utilisation", round((col("count") / DataAccess.participants.count()) * 100))
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

    val compare = DataAccess.participants
      .join(DataAccess.champs, DataAccess.participants("championid") === DataAccess.champs("id"))
      .groupBy("name").count().sort(desc("count"))
      .withColumnRenamed("name", "Champion")

    DataAccess.participants
      .join(DataAccess.champs, DataAccess.participants("championid") === DataAccess.champs("id"))
      .join(DataAccess.stats, DataAccess.participants("id") === DataAccess.stats("id"))
      .filter((DataAccess.participants("player") < 6 and DataAccess.stats("win") === 0) or (DataAccess.participants("player") > 5 and DataAccess.stats("win") === 1))
      .groupBy("name").count().withColumnRenamed("count", "Nombre de victoire")

      .join(compare, DataAccess.champs("name") === compare("Champion"))
      .withColumn("Pourcentage de victoire",round(col("Nombre de victoire")/col("count")*100,2))
      .select("Champion","Nombre de victoire","Pourcentage de victoire").sort(desc("Pourcentage de victoire"))
      .show(countChamp, truncate = false)
  }

  def averageGameTime() {
    println("============Moyenne du temps de jeu d'une partie============")
    DataAccess.matches
      .select(round(avg("duration") / 60, 2))
      .withColumnRenamed("round((avg(duration) / 60), 2)", "Moyenne en minutes")
      .show()
  }

  def playerMap() {
    println("============Répartition des joueurs sur la map============")
    DataAccess.participants
      .groupBy("position").count().sort(desc("count"))
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
      .withColumn("Pourcentage de ban global", round((col("count") / DataAccess.teambans.count()) * 100))
      .withColumn("Pourcentage de ban par game", round((col("count") / DataAccess.matches.count()) * 100))
      .withColumnRenamed("name", "Champion")
      .sort(desc("Pourcentage de ban par game"))
      .show(countChamp, truncate = false)
  }

  def averageKill() {
    println("============Moyenne des kills par joueur============")
    DataAccess.stats
      .select(round(avg("kills"), 2),round(avg("assists"), 2))
      .withColumnRenamed("round(avg(kills), 2)", "Moyenne de kill par joueur")
      .withColumnRenamed("round(avg(assists), 2)", "Moyenne d'assists par joueur")
      .show(truncate = false)
  }

  def Champion() {
    println("============Top des kills / morts par champions ============")
    print("Merci d'indiquer le nom de champion à afficher: ")
    var countChamp = readInt()

    val menu = "src/main/text/menuChamp.txt"
    for (line <- Source.fromFile(menu).getLines) {
      println(line)
    }
    print("Merci d'indiquer l'information à consulter: ")

    var column = ""
    var choice = readInt()
    choice match {
      case 1 => column = "Nombre de kill"
      case 2 => column = "Nombre de morts"
      case 3 => column = "Moyenne de kill par partie"
      case 4 => column = "Nombre de quadrakills"
      case 5 => column = "Nombre de pentakills"
    }

    DataAccess.stats
      .join(DataAccess.participants, DataAccess.participants("id") === DataAccess.stats("id"))
      .join(DataAccess.champs, DataAccess.participants("championid") === DataAccess.champs("id"))
      .toDF()
      .groupBy("name").agg(sum("kills"), sum("quadrakills"), sum("pentakills"),sum("deaths"),count("name"))
      .withColumn("Moyenne de kill par partie", round((col("sum(kills)") / col("count(name)"))))
      .withColumnRenamed("name", "Champion")
      .withColumnRenamed("sum(quadrakills)", "Nombre de quadrakills")
      .withColumnRenamed("sum(pentakills)", "Nombre de pentakills")
      .withColumnRenamed("sum(kills)", "Nombre de kill")
      .withColumnRenamed("sum(deaths)", "Nombre de morts")
      .sort(desc(column))
      .select("Champion", "Nombre de kill", "Nombre de quadrakills", "Nombre de pentakills", "Moyenne de kill par partie","Nombre de morts")
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
        case 0 => principalInformations()
        case 1 => countChampion()
        case 2 => teamsWins()
        case 3 => championsWins()
        case 4 => averageGameTime()
        case 5 => playerMap()
        case 6 => banChampion()
        case 7 => averageKill()
        case 8 => Champion()
        case 9 => System.exit(0)
        case _ => println("Merci de saisir une valeur existante")
      }
    }
  }

}


