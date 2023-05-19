package app.recommender

import app.recommender.LSH.{LSHIndex, NNLookup}
import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext,
                  index: LSHIndex,
                  ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {

  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)

  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    val movies = nn_lookup.lookupSingle(genre)

    println("Movies Found: ")
    movies.foreach(println)

    val watchedMovies = baselinePredictor.listWatchedMovies(userId)
    val moviesToSort = movies.filter(x => !watchedMovies.contains(x._1))
    val ratingsPred = moviesToSort.map(x => (x._1, baselinePredictor.predict(userId, x._1)))
    val ratingsSorted = ratingsPred.sortBy(-_._2)
    ratingsSorted.take(K)

//        val RDDGenre = sc.parallelize(List(genre))
//        val movies = nn_lookup.lookup(RDDGenre).flatMap(_._2)
//        val watchedMovies = baselinePredictor.listWatchedMovies(userId)
//        val moviesToSort = movies.filter(x => !watchedMovies.contains(x._1))
//        val ratingsPred = moviesToSort.map(x => (x._1, baselinePredictor.predict(userId, x._1)))
//        ratingsPred.take(K)
  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = ???
}
