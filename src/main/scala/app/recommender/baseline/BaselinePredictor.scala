package app.recommender.baseline

import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {

  private var state = null
  private var ratingUserBased: RDD[(Int, Double)] = null
  private var ratingMovieBased: RDD[(Int, Double)] = null

  def scale(x: Double, avg: Double): Double = {
      if (x > avg)  5.0 - avg
      else if (x < avg)  avg - 1.0
           else  1.0
  }

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
              //          [(uid, mid, previous rating,  rating, timestamp)]
    /* TODO:
      * 1. Calculate the average rating for each user
      * 2. Calculate the derivation of each user's rating from the average
      * 3. Normalize the derivation, divide by scale(x, avg)
     */
      val ratingUserMovie = ratingsRDD.groupBy(_._1)
                               .mapValues(x => {
                                  val moviesRating = x.groupBy(_._2).mapValues(x => x.toList
                                                                                      .maxBy(_._5)
                                                                                      ._4)
                                                                    .toList
                                  moviesRating
                               })

      val ratingUserAvg = ratingUserMovie.mapValues(x => x.map(_._2).sum / x.length )
      val ratingNormalized = ratingUserMovie.mapValues(x => {
                              //              x: List of (Mid, LastRating)
                                          val avg = x.map(_._2).sum / x.length
                                          val normed = x.map(y => (y._1,  (y._2 - avg) / scale(y._2, avg)))
                                          normed
                                      })
      ratingUserBased = ratingUserAvg

      val ratingFlatten = ratingNormalized.flatMapValues(x => x).map(x => (x._1, x._2._1, x._2._2))
      //               [(uid, mid, normalized rating)]
      val ratingGroupedByMovie = ratingFlatten.groupBy(_._2)
      val ratingMovieAvg = ratingGroupedByMovie.map(x => {
              val mid = x._1
              val sum = x._2.map(_._3).sum
              val cnt = x._2.size
              (mid, sum / cnt)
      })
      ratingMovieBased = ratingMovieAvg
  }

  def predict(userId: Int, movieId: Int): Double = {
    val r_u = ratingUserBased.lookup(userId).head
    val r_m = ratingMovieBased.lookup(movieId).head
    r_u + r_m * scale((r_u + r_m), r_u)
  }
}
