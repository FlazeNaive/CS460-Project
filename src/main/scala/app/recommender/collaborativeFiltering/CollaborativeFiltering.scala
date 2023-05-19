package app.recommender.collaborativeFiltering

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  // NOTE: set the parameters according to the project description to get reproducible (deterministic) results.
  private val maxIterations = 20
  private var model: MatrixFactorizationModel  = null

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val ratingUserMovie = ratingsRDD.groupBy(_._1)
      .flatMapValues(x => {
        val moviesRating = x.groupBy(_._2).mapValues(x => x.toList
          .maxBy(_._5)
          ._4)
          .toList
        moviesRating
      })

    val ratings = ratingUserMovie.map(x => Rating(x._1, x._2._1, x._2._2))

    model = ALS.train(ratings,
                      rank,
                      maxIterations,
                      regularizationParameter,
                      n_parallel,   // n_parallel
                      seed)
  }
  def predict(userId: Int, movieId: Int): Double = {
    model.predict(userId, movieId)
  }

}
