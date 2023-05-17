package app

import app._
import org.apache.spark.{SparkConf, SparkContext}
import loaders.MoviesLoader
import loaders.RatingsLoader
import analytics.SimpleAnalytics
import org.apache.log4j.{Level, Logger}


// flz: debug, finding which folders are accessable
// import java.nio.file.{FileSystems, Files}
// import scala.collection.JavaConverters._
// import scala.io.Source

object Main {
  def main(args: Array[String]):Unit = {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    Logger.getLogger("org").setLevel(Level.OFF)
    print("start\n\n\n\n\n")

    // flz: debug, finding which folders are accessable
    // val dir = FileSystems.getDefault.getPath("./src/main/resources")
    // Files.list(dir).iterator().asScala.foreach(println)

    val resource_path = ""//"./src/main/resources"
    val movies_filename = "movies_small.csv"
    val ratings_filename = "ratings_small.csv"
    // flz: debug, finding which folders are accessable
    //   for (line <- Source.fromFile(resource_path + "/" + filename).getLines) {
    //     println(line)
    //   }

    val moviesLoader = new MoviesLoader(sc, resource_path + "/" + movies_filename)
    val movies_rdd = moviesLoader.load()
    print("done loading movies\n\n\n\n\n")
    val ratingsLoader = new RatingsLoader(sc, resource_path + "/" + ratings_filename)
    val ratings_rdd = ratingsLoader.load()
    print("done loading ratings\n\n\n\n\n")
    // flz: debug output
    // ratings_rdd.collect().foreach(f => println(f._1, f._2, f._3, f._4, f._5))

    val analytics = new SimpleAnalytics()
    analytics.init(ratings_rdd, movies_rdd)
    println("done initializing analytics\n\n\n\n\n")

     analytics.getNumberOfMoviesRatedEachYear.collect().sortWith(_._1 <= _._1).foreach(println)
     println("done calculating number of ratings each yeas\n\n\n\n\n")
     analytics.getMostRatedMovieEachYear.filter(_._1 == 1996).collect().foreach(println)
     println("done looking up for most rated MOVIE each yeas\n\n\n\n\n")
     analytics.getMostRatedGenreEachYear.filter(_._1 == 1996).collect().foreach(println)
     println("done looking up for most rated GENRE each yeas\n\n\n\n\n")
    // val most_least_rated_genre = analytics.getMostAndLeastRatedGenreAllTime
    // println("MOST and LEAST rated GENRE all time: ")
    // println(most_least_rated_genre._1, most_least_rated_genre._2)
    // println("done looking up for MOST and LEAST rated GENRE all time\n\n\n\n")
    // val test_genre_list = sc.parallelize(List("Drama", "Comedy"))
    // analytics.getAllMoviesByGenre(movies_rdd, (test_genre_list))
    // println("done looking up for ALL movies by GENRE\n\n\n\n\n")


    //your code goes here
  }
}
