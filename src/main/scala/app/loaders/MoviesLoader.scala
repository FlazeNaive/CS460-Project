package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    val distFile = sc.textFile("./src/main/resources/"+path)
    val rdd = distFile.map(f=>{f.split("\n")})
                      .flatMap(f=>{f.map(f=>{f.split("\"")})})

    return rdd.map(f => {(f(0).dropRight(1).toInt, f(1), f(3).split("\\|").toList)})
  }
}

