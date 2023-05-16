package app

import app._
import org.apache.spark.{SparkConf, SparkContext}
import loaders.MoviesLoader

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
    val filename = "movies_small.csv"
    // flz: debug, finding which folders are accessable
    //   for (line <- Source.fromFile(resource_path + "/" + filename).getLines) {
    //     println(line)
    //   }

    val rdd = sc.textFile(resource_path + "/" + filename) 
    // val moviesLoader = new MoviesLoader(sc, "dataset/movies_small.csv")
    print("done collecting\n\n\n\n\n")
    // val rdd = moviesLoader.load()
    // print("done loading\n\n\n\n\n")
    print(rdd.collect().mkString("\n"))
    //your code goes here
  }
}
