package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.io.Source

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
      val filepath = "./src/main/resources/"+path

      val lines = Source.fromFile(filepath).getLines.toList
      val ratings = lines.map(line => {
        val fields = line.split("\\|")
        (fields(0).toInt, fields(1).toInt, fields(2).toDouble, fields(3).toInt)
        // (user_id: Int, title_id: Int, rating: Double, timestamp: Int)
      })
      val usrRatings = ratings.groupBy(_._1).mapValues(x => x.sortWith(_._4 < _._4))
      val usrRatingGroupbyMovie = usrRatings.mapValues(x => x.groupBy(_._2))
      val usrRatingWithPre = usrRatingGroupbyMovie.mapValues( x => {
        val pre = x.mapValues(y => {
          // x: (title_id: Int, List[(user_id: Int, title_id: Int, rating: Double, timestamp: Int)])
          // y: List[(user_id: Int, title_id: Int, rating: Double, timestamp: Int)]
          val pre = y.map(_._3)
          val pre2 = pre.zip(pre.tail).map(xxx => (xxx._1, Some(xxx._2)))
          val pre3 = pre2 :+ (pre.last, None)
          y.zip(pre3).map(xxx => (xxx._1._1, xxx._1._2, xxx._2._2, xxx._1._3, xxx._1._4))
          //                      (uid,      title_id,  pre_rating, rating,     timestamp)
        }).flatMap(_._2).toList
        pre
      }).flatMap(_._2).toList
      val sort_for_test = usrRatingWithPre.sortBy(x => (x._1.toString, x._2.toString))
      sc.parallelize(usrRatingWithPre)
//    val distFile = sc.textFile("./src/main/resources/"+path)
//    val rdd = distFile.map(f=>{f.split("\n")})
//                      .flatMap(f=>{f.map(f=>{f.split("\\|")})})
//
//    return rdd.map(f => {(f(0).toInt, f(1).toInt, None, f(2).toDouble, f(3).toInt) })
  }
}