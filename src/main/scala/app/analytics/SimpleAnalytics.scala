package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime


class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null

  private var titlesGroupedByID: RDD[(Int, Iterable[(String, List[String])])] = null
  private var ratingsGroupedByYearByTitle: RDD[(Int, Map[Int,Iterable[(Int, Option[Double], Double, Int)]])] = null

  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {
            ratingsPartitioner = new HashPartitioner(ratings.partitions.length)
            moviesPartitioner = new HashPartitioner(movie.partitions.length)

            val paired_movie = movie.map(m => (m._1, (m._2, m._3)))
            val partitioned_movie = paired_movie.partitionBy(moviesPartitioner).persist(MEMORY_AND_DISK)
            titlesGroupedByID = partitioned_movie.groupByKey()


            val paired_ratings = ratings.map(r => 
                                        (new DateTime(r._5.toLong * 1000).getYear, 
                                                      (r._2, 
                                                        // (r._1, r._2, r._3, r._4, r._5))))
                                                        (r._1, r._3, r._4, r._5))))
            val partitioned_ratings = paired_ratings.partitionBy(ratingsPartitioner).persist(MEMORY_AND_DISK)
            val grouped_year_ratings = partitioned_ratings.groupByKey()

            ratingsGroupedByYearByTitle = grouped_year_ratings.map({
                                              case (year, titleCommentsIterable) => 
                                                val ratingsByTitle = titleCommentsIterable.groupBy(_._1)
                                                val groupedByTitle = ratingsByTitle.map { case (title, commentsIterable) =>
                                                                                            (title, commentsIterable.map(_._2))
                                                                                        }
                                                (year, groupedByTitle)
                                            })
              
            println("done grouping")
            
          }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = ratingsGroupedByYearByTitle.map({case (a, b) => (a, b.size)})

  def helper_getMostRatedIDEachYear: RDD[(Int, Int)] = ratingsGroupedByYearByTitle.map({case (year, titleCommentsMap) => 
                                                  val groupedByTitle = titleCommentsMap.map({case (title, commentsIterable) => 
                                                                                                  (title, commentsIterable.size)
                                                                                               })
                                                   val maxRatedTitleID = groupedByTitle.maxBy(x => (x._2, x._1)) // (titleID, numComments)
                                                   (maxRatedTitleID._1, year)
                                                 }) // (titleID, year)

  def getMostRatedMovieEachYear: RDD[(Int, String)] = {
                              val maxRatedTitleID = helper_getMostRatedIDEachYear
                              val maxRatedTitle = maxRatedTitleID.join(titlesGroupedByID).map({case (titleID, (year, titleIterable)) => 
                                                                                                  val title = titleIterable.head._1
                                                                                                  (year, title)
                                                                                                })
                              // maxRatedTitle.collect().foreach(println)
                              maxRatedTitle
                            }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {
                              val maxRatedTitleID = helper_getMostRatedIDEachYear
                              val maxRatedGenre= maxRatedTitleID.join(titlesGroupedByID).map({case (titleID, (year, titleIterable)) => 
                                                                                                  val genre = titleIterable.head._2
                                                                                                  (year, genre)
                                                                                                })
                              // maxRatedGenre.collect().foreach(println)
                              maxRatedGenre
  }

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = {
                              val genreEachYear = getMostRatedGenreEachYear.flatMap(a => a._2)
                                                                           .map((_, 1))
                                                                           .reduceByKey(_ + _) // (genre, numRatings)
                              // flz: debug output
                              // genreEachYear.collect().foreach(println)
                              val mostRatedGenre = genreEachYear.reduce((a, b) => if (a._2 == b._2) 
                                                                                    if (a._1 < b._1) a else b
                                                                                    else if (a._2 > b._2) a else b)
                              val leastRatedGenre = genreEachYear.reduce((a, b) => if (a._2 == b._2)
                                                                                    if (a._1 < b._1) a else b
                                                                                    else if (a._2 < b._2) a else b)
                              // flz: debug output
                              // println("mostRatedGenre: ", mostRatedGenre)
                              // println("leastRatedGenre: ", leastRatedGenre)
                              (mostRatedGenre, leastRatedGenre)
  }

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

