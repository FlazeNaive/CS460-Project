package app

import app._
import org.apache.spark.{SparkConf, SparkContext}
import loaders.MoviesLoader
import loaders.RatingsLoader

// flz: debug, finding which folders are accessable
// import java.nio.file.{FileSystems, Files}
// import scala.collection.JavaConverters._
// import scala.io.Source

object Main {
  def main(args: Array[String]):Unit = {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    print("start\n\n\n\n\n")

    // flz: debug, finding which folders are accessable
    // val dir = FileSystems.getDefault.getPath("./src/main/resources")
    // Files.list(dir).iterator().asScala.foreach(println)

    val resource_path = "./src/main/resources"
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

    //your code goes here
  }
}
