package app.recommender.baseline

import org.apache.spark.rdd.RDD

import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.apache.spark.HashPartitioner

class BaselinePredictor() extends Serializable {

  private var state = null
  private var ratingUserBased: RDD[(Int, Double)] = null
  private var ratingMovieBased: RDD[(Int, Double)] = null
  private var ratingUserMovie: RDD[(Int, List[(Int, Double)])] = null

  private var partitionerUserBased:  HashPartitioner = null
  private var partitionerMovieBased: HashPartitioner = null
  private var partitionerUserMovie:  HashPartitioner = null

  def scale(x: Double, avg: Double): Double = {
      if (x > avg)  5.0 - avg
      else if (x < avg)  avg - 1.0
           else  1.0
  }

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
              //          [(uid, mid, previous rating,  rating, timestamp)]
     /*
      * 1. Calculate the average rating for each user
      * 2. Calculate the derivation of each user's rating from the average
      * 3. Normalize the derivation, divide by scale(x, avg)
      */
      ratingUserMovie = ratingsRDD.groupBy(_._1)
                               .mapValues(x => {
                                  val moviesRating = x.groupBy(_._2).mapValues(x => x.toList
                                                                                      .maxBy(_._5)
                                                                                      ._4)
                                                                    .toList
                                  moviesRating
                               })
      partitionerUserMovie = new HashPartitioner(ratingUserMovie.partitions.length)
      ratingUserMovie = ratingUserMovie.partitionBy(partitionerUserMovie).persist(MEMORY_AND_DISK)

      val ratingUserAvg = ratingUserMovie.mapValues(x => x.map(_._2).sum / x.length )
      val ratingNormalized = ratingUserMovie.mapValues(x => {
                              //              x: List of (Mid, LastRating)
                                          val avg = x.map(_._2).sum / x.length
                                          val normed = x.map(y => (y._1,  (y._2 - avg) / scale(y._2, avg)))
                                          normed
                                      })
      ratingUserBased = ratingUserAvg
      partitionerUserBased = new HashPartitioner(ratingUserBased.partitions.length)
      ratingUserBased = ratingUserBased.partitionBy(partitionerUserBased).persist(MEMORY_AND_DISK)

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
      partitionerMovieBased = new HashPartitioner(ratingMovieBased.partitions.length)
      ratingMovieBased = ratingMovieBased.partitionBy(partitionerMovieBased).persist(MEMORY_AND_DISK)
  }

  def predict(userId: Int, movieId: Int): Double = {
    val r_u = ratingUserBased.lookup(userId).head
    val r_m = ratingMovieBased.lookup(movieId).head
    r_u + r_m * scale((r_u + r_m), r_u)
  }

  def listWatchedMovies(userId: Int): List[Int] = {
    ratingUserMovie.lookup(userId).head.map(_._1)
  }
}
