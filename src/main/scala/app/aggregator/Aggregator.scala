package app.aggregator

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private var state = null
  private var partitioner: HashPartitioner = null
  private var aggregated: RDD[(Int, (String, List[String], List[(Int, List[(Int, Option[Double], Double, Int)])], Double))] = null

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            //           (uid, tid, old_rating, rating, timestamp)
            title: RDD[(Int, String, List[String])]
          ): Unit = {
              val ratingGroupByTitle = ratings.groupBy(_._2)
              val titleGroupByTitle = title.map(x => (x._1, x))
              val joined = titleGroupByTitle.leftOuterJoin(ratingGroupByTitle).map( x => {
                    val tid = x._1
                    val title = x._2._1._2
                    val keywords = x._2._1._3

                    val comments = x._2._2 match {
                      case None => List()
                      case Some(yy) =>
                                        yy.map(y => (y._1, y._3, y._4, y._5))
                                                  //(uid, prev_rating, rating, timestamp)
                                          .groupBy(_._1)
                                          .mapValues(x => {
                                                  // x is the comments from the same user
                                              val sorted = x.toList.sortBy(_._4)
                                              sorted
                                          }).toList
                    }
                    var avg = 0.0
                    if (comments.size > 0) {
                      val sum = comments.map(x => x._2.last._3).sum
                      avg = sum / comments.size
                    }

                (tid, (title, keywords, comments, avg))
              }) // joined: (tid, title, keywords, a map from uid to list of SORTED comments, average rating)
              partitioner = new HashPartitioner(joined.partitions.length)
              aggregated = joined.partitionBy(partitioner).persist(MEMORY_AND_DISK)
          }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = aggregated.map(x => (x._2._1, x._2._4))

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    val filterByKey = aggregated.filter( x => {keywords.forall(x._2._2.contains(_))})
    if (filterByKey.count() == 0)
      return 1.0
    val filterByRating = filterByKey.filter(x => x._2._4 > 0)
    if (filterByRating.count() == 0)
      return 0.0
    val sum = filterByRating.map(x => x._2._4).sum
    val count = filterByRating.count()
    sum / count
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = ???
}
