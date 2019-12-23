import org.apache.spark.{SparkConf, SparkContext}

object DecisionTreeLearning {

  /**
   * Case class that represents a review object from the variables that have a predictive value following some criteria
   *
   * @param marketplace      position 0
   * @param verifiedPurchase position 11
   * @param starRating       position 7
   * @param vine             position 10
   * @param product_category position 6
   * @param helpfulVotes     position 8
   */
  case class Review(marketplace: String,
                    verifiedPurchase: Boolean,
                    starRating: Int,
                    vine: String,
                    product_category: Int,
                    helpfulVotes: Int)

  /**
   * Parse a review (A line of the data set) and return a Review Object
   *
   * @param review review of a product
   * @return
   */
  def parseReview(review: String): Review = {
    val reviewTokenized = review.split("\t")
    val parsed = Review(reviewTokenized(0), reviewTokenized(11).toBoolean, reviewTokenized(7).toInt, reviewTokenized(10), reviewTokenized(6).toInt, reviewTokenized(8).toInt)
    parsed
  }

  def main(args: Array[String]): Unit = {
    println("Here is where the magic begins")
    val conf = new SparkConf()
      .setAppName("Niklaus decision tree") // Application name
      .setMaster("local[1]") // Acts as a master node with 1 thread
    val sc = new SparkContext(conf)
    println(sc)

    /**
     * reads a file from Hadoop Distributed File System returns an RDD of Strings
     */
    //    val loadedData = sc.textFile("/Volumes/ClaudiaDrive/amazon_reviews_us_Musical_Instruments_v1_00.tsv")
    val loadedData = sc.textFile("./data/smaller.tsv")
    val count = loadedData.count()
    println(count)

    val mappedData = loadedData.map(parseReview)
      .collect()
      .take(2)
    //      //      .foreach(line => println(line(0) + " \t" + line(1) + " \t" + line(2) + " \t" + line(3) + "\t" + line(4)))
    //      .foreach(line => {
    //        line.foreach(word => print(word.slice(0, 10) + "\t\t"))
    //        println()
    //      })

  }
}
