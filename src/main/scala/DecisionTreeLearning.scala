import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object DecisionTreeLearning {
  type Dataset = RDD[Attributes]
  type AttributeId = Int
  val threshold = 0.5

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
    val reviewTokenized: Array[String] = review.split("\t") // Split review and get each element since we know it is a tab separated file
    val verifiedPurchase: Boolean = reviewTokenized(11) == "Y" // Transform verified purchase into a boolean variable, if the element equals Y then is true, false otherwise
    val vine = reviewTokenized(10) == "Y" // Transform vine into a boolean variable
    val parsed = Review(reviewTokenized(0), verifiedPurchase, reviewTokenized(7).toInt, vine, reviewTokenized(6), reviewTokenized(13), reviewTokenized(9).toInt, reviewTokenized(8).toInt)
    println("marketplace: ", parsed.marketplace)
    println("verifiedPurchase: ", parsed.verifiedPurchase)
    println("starRating: ", parsed.starRating)
    println("vine: ", parsed.vine)
    println("product_category: ", parsed.product_category)
    println("review_body: ", parsed.review_body)
    println("total_votes: ", parsed.total_votes)
    println("helpfulVotes: ", parsed.helpfulVotes)
    println("-------------")
    return parsed
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
    val value: Boolean = (helpfulVotes.toDouble / totalVotes.toDouble) > threshold

    def possibleValues(): Array[Boolean] = Array(true, false)
  }

  class BooleanAttr(val v: Boolean) extends Attribute[Boolean] {
    val value: Boolean = v

    def possibleValues(): Array[Boolean] = Array(true, false)
  }

  def extractAttributes(reviews: RDD[Review]): RDD[Attributes] = {
    return reviews.map((review) => Attributes(
      new BooleanAttr(review.verifiedPurchase),
      new Rating(review.starRating),
      new BooleanAttr(review.vine),
      review.product_category,
      new ReviewBody(review.review_body),
      new HelpfulReview(review.helpfulVotes, review.total_votes)
    ))
    //    val verifiedPurchase = new BooleanAttr(reviews.verifiedPurchase)
    //    val starRating = new Rating(reviews.starRating)
    //    val vine = new BooleanAttr(reviews.vine)
    //    val body = new ReviewBody(reviews.review_body)
    //    val helpfulReview = new HelpfulReview(reviews.helpfulVotes, reviews.total_votes)
    //    val attributes = Attributes(verifiedPurchase, starRating, vine, reviews.product_category, body, helpfulReview)
    //    attributes
  }

  /**
   * Compute the entropy of a given data set and a target attribute
   *
   * @param data
   * @param target
   * @return
   */
  def entropy(data: RDD[Attributes], target: AttributeId): Float = {
    // here we have two options, either we compute the total number of elements by doing a count
    // Add the end we only need, the number of positive and negative values
    //    data.reduce()
    val count = data.count()
    return 2

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
    val loadedData: RDD[String] = sc.textFile("./data/smaller.tsv") // This is a transformation since, it returns an RDD
    val count: Long = loadedData.count() // Possible to persist
    println("Number of elements :)")
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
      .map(parseReview) // At this point the code is not executed since we only do a map
//      .take(2)
    //      .persist()


        val e = extractAttributes(mappedData)
//    e.take(2)

    //    entropy(e, 6)


    println("Print first")
    println(mappedData)

    //    System.in.read()

  }
}

