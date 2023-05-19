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
//  private var aggregated: RDD[(Int, (String, List[String], Double, Int))] = null
  private var aggregated: RDD[(Int, (String, List[String],
                                    List[(Int, List[(Int, Option[Double], Double, Int)])],
                                          Double, Int))] = null
//                             (tid, (title, keywords, comments,                                sum, count))
//                                                      (uid, prev_rating, rating, timestamp)

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
                // x is the data of one movie
                //          title, [comments]
                    val tid = x._1
                    val title    = x._2._1._2
                    val keywords = x._2._1._3
//                    var count = x._2._2 match {
//                            // x._2._2: Option[Iterable[(Int, Int, Option[Double], Double, Int)]]
//                            // x._2._2 is the set of comments for this movie
//                      case None => 0
//                      case Some(yy) => yy.size
//                    }
//                    var sum = x._2._2 match {
//                      case None => 0.0
//                      case Some(yy) => {
//                           // yy: set of comments
//                        val only_rating = yy.map(y => y._3 match {
//                           // contribution of comment y, is y._4 - y._2
//                          case None => y._4
//                          case Some(z) => y._4 - z
//                        })
//                        only_rating.sum
//                      }
//                    }
//                (tid, (title, keywords, sum, count))

                    var sum = 0.0
                    val comments = x._2._2 match {
                      case None => List()
                      case Some(yy) =>
                                        yy.map(y => (y._1, y._3, y._4, y._5))
                                                  //(uid, prev_rating, rating, timestamp)
                                          .groupBy(_._1)
                                                // group by Uid
                                                // (uid, List[(uid, prev_rating, rating, timestamp)])
                                          .mapValues(x => {
                                                // x is the List of comments from the same user
                                                      val sorted = x.toList.sortBy(_._4)
                                                      sorted
                                                  })
                                          .toList
                    }
                    if (comments.size > 0) {
//                      sum = comments.map(x => x._2.last._3).sum
                        val sumUID = comments.map(x => {
                          val processed_comments = x._2.map(y => y._3)
//                          val processed_comments = x._2.map(y => y._2 match {
//                            case None => y._3
//                            case Some(z) => y._3 - z
//                          })
                          processed_comments.sum
                        })
                        sum = sumUID.sum
                    }
//                      else {
//                      println("\n\n\n\n" + title)
//                      println("\nNOOOOOOOOO COMMMENTTTT\n\n\n")
//
//                    }
//                if (tid == 6849)
//                  println("\n\n\n\n" + title + " " + sum + " " + comments.size + "\n\n\n")
                (tid, (title, keywords, comments, sum, comments.size))
              }) // joined: (tid, title, keywords, a map from uid to list of SORTED comments, average rating)
              partitioner = new HashPartitioner(joined.partitions.length)
              val tmp = joined.partitionBy(partitioner).persist(MEMORY_AND_DISK)
              aggregated = tmp
          }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
//  def getResult(): RDD[(String, Double)] = aggregated.map(x => (x._2._1, x._2._3 / x._2._4))
  def getResult(): RDD[(String, Double)] = aggregated.map(x => {
                                                  if (x._2._5 == 0)
                                                            (x._2._1, 0.0)
                                                  else (x._2._1, x._2._4 / x._2._5)
                                            })

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
      return -1.0
    val filterByRating = filterByKey.filter(x => x._2._5 > 0)
    if (filterByRating.count() == 0)
      return 0.0
//    val sum = filterByRating.map(x => x._2._3 / x._2._4).sum
    val sum = filterByRating.map(x => x._2._4 / x._2._5).sum
                  //             x is data for each movie
    val count = filterByRating.count()
    sum / count
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {
                               // (uid, tid, old_rating, rating, timestamp)
    val delta = sc.parallelize(delta_)
    val deltaGroupByTitle = delta.groupBy(_._2)
        // (tid, (uid, tid, old_rating, rating, timestamp))

    val joined_raw = aggregated.leftOuterJoin(deltaGroupByTitle)

    val joined = joined_raw.map(x => x._2._2 match {
            // x._1 : tid
            // x._2 : joined values
            //      x._2._1: original values of this movie
            //          (title, keywords, comments, avg, count of comments)
            //      x._2._2: new comments
            //          (uid, tid, old_rating, rating, timestamp)

      case None => (x._1, x._2._1)    // keep the original values
      case Some(new_Comments) => {
        //     [(uid, tid, old_rating, rating, timestamp)]
        val ori = x._2._1
        val sum = ori._4

        val to_add = new_Comments.map(y => y._3 match {
                          case None => y._4
                          case Some(z) => y._4 - z
                        })
        val new_sum = sum + to_add.sum
        val delta_count = new_Comments.map(y => y._3 match {
                          case None => 1
                          case Some(z) => 0
                        }).sum

        (x._1, (ori._1, ori._2, ori._3, new_sum, ori._5 + delta_count))
      }
    })
    aggregated = joined.partitionBy(partitioner).persist(MEMORY_AND_DISK)
  }
}
