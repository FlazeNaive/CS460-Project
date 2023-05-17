package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime


class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null

  // public var titlesGroupedByID
  // public var ratingsGroupedByYearByTitle

  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {
            // first partition the RDD
            ratingsPartitioner = new HashPartitioner(ratings.partitions.length)
            moviesPartitioner = new HashPartitioner(movie.partitions.length)

            val paired_movie = movie.map(m => (m._1, (m._2, m._3)))
            val partitioned_movie = paired_movie.partitionBy(moviesPartitioner).persist(MEMORY_AND_DISK)
            val grouped_movies = partitioned_movie.groupByKey()
            // flz: debug output for grouped_movies
            grouped_movies.collect().foreach(f => println(f._1, f._2))


            val paired_ratings = ratings.map(r => 
                                        (new DateTime(r._5.toLong * 1000).getYear, 
                                                      (r._2, 
                                                        // (r._1, r._2, r._3, r._4, r._5))))
                                                        (r._1, r._3, r._4, r._5))))
            val partitioned_ratings = paired_ratings.partitionBy(ratingsPartitioner).persist(MEMORY_AND_DISK)
            val grouped_year_ratings = partitioned_ratings.groupByKey()
            // grouped_year_ratings.collect().foreach(f => println(f._1, f._2))

            val grouped_title_year_ratings = grouped_year_ratings.map({
                                              case (year, titleCommentsIterable) => 
                                                val ratingsByTitle = titleCommentsIterable.groupBy(_._1)
                                                val groupedByTitle = ratingsByTitle.map { case (title, commentsIterable) =>
                                                                                            (title, commentsIterable.map(_._2))
                                                                                        }
                                                (year, groupedByTitle)
                                            })
              
            // flz: debug output for grouped_ratings
            grouped_title_year_ratings.collect().foreach(f => println(f._1, "\n", f._2 get 5378, "\n\n\n"))

            println("done grouping")

            
          }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = ???

  def getMostRatedMovieEachYear: RDD[(Int, String)] = ???

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = ???

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = ???

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = ???

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = ???

}

