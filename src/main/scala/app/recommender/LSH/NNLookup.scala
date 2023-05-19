package app.recommender.LSH

import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookup(lshIndex: LSHIndex) extends Serializable {

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, resut) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    val tagsHashed = lshIndex.hash(queries)
    val queryData = lshIndex.lookup(tagsHashed)
    queryData.map(x => (x._2, x._3))
  }
  def lookupSingle(query: List[String]): List[(Int, String, List[String])] = {
    val tagsHashed = lshIndex.hashSingle(query)
    val queryData = lshIndex.lookupSingle(tagsHashed)
    queryData._3
  }
}
