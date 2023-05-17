package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {
    val distFile = sc.textFile("./src/main/resources/"+path)
    val rdd = distFile.map(f=>{f.split("\n")})
                      .flatMap(f=>{f.map(f=>{f.split("\\|")})})
    
    return rdd.map(f => {(f(0).toInt, f(1).toInt, None, f(2).toDouble, f(3).toInt) })
  }
}