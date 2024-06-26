package app.recommender.LSH


import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/**
 * Class for indexing the data for LSH
 *
 * @param data The title data to index
 * @param seed The seed to use for hashing keyword lists
 */
class LSHIndex(data: RDD[(Int, String, List[String])], seed : IndexedSeq[Int]) extends Serializable {

  private val minhash = new MinHash(seed)

  /**
   * Hash function for an RDD of queries.
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]) : RDD[(IndexedSeq[Int], List[String])] = {
    input.map(x => (minhash.hash(x), x))
  }
  def hashSingle(input: List[String]) : (IndexedSeq[Int], List[String]) = {
    (minhash.hash(input), input)
  }

  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */
  def getBuckets(): RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {
    val tagsExtract = data.map(_._3)
    val tagsHashed = hash(tagsExtract)
    val dataWithHash = tagsHashed.map(_._1).zip(data) // data.zip(tagsHashed)
    val dataGrouped = dataWithHash.groupByKey().map(x => (x._1, x._2.toList))
    val dataPartitioned = dataGrouped.partitionBy(new HashPartitioner(4)).persist(MEMORY_AND_DISK)
    dataPartitioned
  }

  /**
   * Lookup operation on the LSH index
   *
   * @param queries The RDD of queries. Each query contains the pre-computed signature
   *                and a payload
   * @return The RDD of (signature, payload, result) triplets after performing the lookup.
   *         If no match exists in the LSH index, return an empty result list.
   */
  def lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)])
  : RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])] = {
    val dataPartitioned = getBuckets()
    val queryDataRaw = queries.join(dataPartitioned)
    val queryData = queryDataRaw.map(x => (x._1, x._2._1, x._2._2))
//    val queryData = queries.map(x => {
//      val signature = x._1
//      val payload = x._2
//      val result = dataPartitioned.lookup(signature).flatten.toList
//      (signature, payload, result)
//    })
    queryData
  }

  def lookupSingle[T: ClassTag](query: (IndexedSeq[Int], T))
  : (IndexedSeq[Int], T, List[(Int, String, List[String])]) = {
    val dataPartitioned = getBuckets()
    val queryDataRaw = dataPartitioned.lookup(query._1).flatten.toList
    val queryData = (query._1, query._2, queryDataRaw)
    queryData
  }
}
