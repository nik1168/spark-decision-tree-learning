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
   * @param review_body      position 13
   * @param total_votes      position 9
   * @param helpfulVotes     position 8
   */
  case class Review(marketplace: String,
                    verifiedPurchase: Boolean,
                    starRating: Int,
                    vine: Boolean,
                    product_category: String,
                    review_body: String,
                    total_votes: Int,
                    helpfulVotes: Int)

  /**
   * Parse a review (A line of the data set) and return a Review Object
   *
   * @param review review of a product
   * @return Review object
   */
  def parseReview(review: String): Review = {
    val reviewTokenized = review.split("\t")
    val verifiedPurchase = reviewTokenized(11) == "Y"
    val vine = reviewTokenized(10) == "Y"
    val parsed = Review(reviewTokenized(0), verifiedPurchase, reviewTokenized(7).toInt, vine, reviewTokenized(6), reviewTokenized(13), reviewTokenized(9).toInt, reviewTokenized(8).toInt)
    parsed
  }

  /**
   * Abstract class to define the methods an attribute should have, in this case the possible values
   *
   * @tparam T
   */
  abstract class Attribute[T] {
    def possibleValues(): Array[T]
  }

  class Rating(val n: Int) extends Attribute[Int] {
    val value: Int = n

    def possibleValues(): Array[Int] = Array(1, 2, 3, 4, 5)
  }

  class ReviewBody(val body: String) extends Attribute[String] {
    val value: String = body

    def possibleValues(): Array[String] = Array("Q1", "Q2", "Q3")
  }

  class HelpfulReview(val helpfulVotes: Int, val totalVotes: Int) {
    val threshold: Double = 0.5
    val value: Boolean = (helpfulVotes.toDouble / totalVotes.toDouble) > threshold

    def possibleValues(): Array[Boolean] = Array(true, false)
  }

  class BooleanAttr(val v: Boolean) {
    val value: Boolean = v

    def possibleValues(): Array[Boolean] = Array(true, false)
  }

  case class Attributes(verifiedPurchase: BooleanAttr,
                        starRating: Rating,
                        vine: BooleanAttr,
                        product_category: String,
                        body: ReviewBody,
                        helpfulReview: HelpfulReview
                       )

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
    //   val loadedData = sc.textFile("/Volumes/ClaudiaDrive/amazon_reviews_us_Musical_Instruments_v1_00.tsv")
    val loadedData = sc.textFile("./data/smaller.tsv")
    val count = loadedData.count()
    println(count)

    /**
     * mapPartitionsWithIndex (transformation)
     * map (transformation)
     * first (action :P)
     * foreach (action)
     * There is only one stage in the visualization graph since, up until this point there isn't shuffling
     */
    val mappedData = loadedData
      .mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter) // We use this to remove the header of the data set, see if this is the best solution, other option will be to use https://intellipaat.com/community/7382/how-do-i-skip-a-header-from-csv-files-in-spark
      .map(parseReview)
//      .take(50)
    println("Print first")
    println(mappedData)

    System.in.read()

  }
}
