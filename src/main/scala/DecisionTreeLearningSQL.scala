import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.util.control.Breaks._

object DecisionTreeLearningSQL {
  type Dataset = RDD[Attributes]
  type AttributeId = Int
  val threshold = 0.5
  var attr_name_info_gain: mutable.Map[String, Double] = collection.mutable.Map[String, Double]()
  val conf: SparkConf = new SparkConf()
    .setAppName("Niklaus decision tree") // Application name
//    .setMaster("local[1]") // Acts as a master node with 1 thread

  val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()

//  Logger.getLogger("org").setLevel(Level.OFF)
//  Logger.getLogger("akka").setLevel(Level.OFF)

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

  // For some reason I think this is not correct, because ok, you know, the values that each review shoud take
  // But that's it, what we need to understand is the following, out of all of the attributes, or columns,
  // Which values can take each column

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

  def calculate_info_gain(entropy: Double, joined_df: DataFrame, total_elements: Long): Double = {
    var attr_entropy: Double = 0.0
    joined_df.rdd.collect().foreach(anAttributeData => {
      var helpful_class_count = anAttributeData(1).toString.toLong
      var not_helpful_class_count = anAttributeData(2).toString.toLong
      if (helpful_class_count == null) {
        helpful_class_count = 0
      }
      else if (not_helpful_class_count == null) {
        not_helpful_class_count = 0
      }
      val count_of_class = helpful_class_count + not_helpful_class_count
      val classmap = Map("helpful" -> helpful_class_count, "not_helpful" -> not_helpful_class_count)
      attr_entropy = attr_entropy + ((count_of_class.toDouble / total_elements.toDouble) * calculateEntropy(count_of_class, classmap))
    })
    val gain: Double = entropy - attr_entropy
    gain
  }

  def get_attr_info_gain_data_prep(attr: String, data: DataFrame, entropy: Double, total_elements: Long, where_condition: String): Unit = {
    val attr_grp_y: DataFrame = if (where_condition.length == 0) data.filter(col("is_vote_helpful") === true).groupBy(attr).agg(Map("is_vote_helpful" -> "count")).withColumnRenamed("count(is_vote_helpful)", "helpful_count") else data.where("is_vote_helpful=true " + where_condition).groupBy(attr).agg(Map("is_vote_helpful" -> "count")).withColumnRenamed("count(is_vote_helpful)", "helpful_count")
    val attr_grp_n: DataFrame = if (where_condition.length == 0) data.filter(col("is_vote_helpful") === false).groupBy(attr).agg(Map("is_vote_helpful" -> "count")).withColumnRenamed("count(is_vote_helpful)", "not_helpful_count") else data.where("is_vote_helpful=false " + where_condition).groupBy(attr).agg(Map("is_vote_helpful" -> "count")).withColumnRenamed("count(is_vote_helpful)", "not_helpful_count")
//    var joined_df: DataFrame = attr_grp_y.join(attr_grp_n, (col(attr_grp_y.columns(0)) === col(attr_grp_n.columns(0))), "outer")
//      .withColumn("total", col(attr_grp_y.columns(0)) + col(attr_grp_n.columns(0)))
//      .select(attr_grp_y.columns(0), attr_grp_y.columns(1), attr_grp_n.columns(1))
    val joined_df: DataFrame = attr_grp_y.join(attr_grp_n, Seq(col(attr_grp_y.columns(0)).toString()), "outer")
//    val sd = joined_df.na.fill("0")
    val sd = joined_df.na.fill(0,Array[String]("helpful_count","not_helpful_count"))
    val joined_df_not_n: DataFrame = sd
      .withColumn("total", sd("helpful_count") + sd("not_helpful_count"))
//      .select(attr_grp_y.columns(0), attr_grp_y.columns(1), attr_grp_n.columns(1))
//    val joinedDF = attr_grp_y.join(attr_grp_n, Seq(col(attr_grp_y.columns(0)).toString()))
    val gain_for_attribute = calculate_info_gain(entropy, joined_df_not_n, total_elements)
    attr_name_info_gain(attr) = gain_for_attribute
    //    attr_name_info_gain.put(attr,gain_for_attribute)
  }

  def process_data_set(excludedAttrs: List[String], data: DataFrame, helpful: Long, notHelpful: Long, where_condition: String): Unit = {
    val total_elements: Long = helpful + notHelpful
    val subs_info = Map[String, Long]("helpful" -> helpful, "notHelpful" -> notHelpful)
    val entropy = calculateEntropy(total_elements, subs_info)
    println("The entropy is: ", entropy)
    attr_name_info_gain = collection.mutable.Map[String, Double]()

    val attrs = List("marketplace", "verified_purchase", "star_rating", "vine", "product_category")
    attrs.foreach(attr => {
      if (!excludedAttrs.contains(attr)) {
        get_attr_info_gain_data_prep(attr, data, entropy, total_elements, where_condition)
      }
    })
  }

  def log2_val(x: Double): Double = {
    val lnOf2 = scala.math.log(2)
    scala.math.log(x) / lnOf2
  }

  def calculateEntropy(total_elements: Long, elements_in_each_class: Map[String, Long]): Double = {
    val keysInMap: Set[String] = elements_in_each_class.keySet
    var entropy: Double = 0.0
    keysInMap.foreach((key) => {
      breakable {
        val number_of_elements_in_class = elements_in_each_class(key)
        if (number_of_elements_in_class == 0) {
          break
        }
        val ratio: Double = number_of_elements_in_class.toDouble / total_elements.toDouble
        entropy = entropy - (ratio * log2_val(ratio))
      }

    })
    entropy
  }

  def build_tree(max_gain_attr: String, processed_attrs: List[String], data: DataFrame, where_condition: String): Unit = {
    println("Tree papaya")
    val attrValues = ss.sql("SELECT distinct " + max_gain_attr + " FROM dataset  where 1==1 " + where_condition)
    var orig_where_condition = where_condition
    attrValues.rdd.collect().foreach(aValueForMaxGainAttr => {
      breakable {
        var leaf_node = ss.emptyDataFrame
        val adistinct_value_for_attr = aValueForMaxGainAttr(0)
        val new_where_condition = orig_where_condition + " and " + max_gain_attr + "=='" + adistinct_value_for_attr + "'"
        val played_for_attr = ss.sql("select * from dataset where is_vote_helpful==true" + new_where_condition).count()
        val notplayed_for_attr = ss.sql("select * from dataset where is_vote_helpful==false" + new_where_condition).count()
        //        var leaf_values = List[String]()
        if (played_for_attr == 0 || notplayed_for_attr == 0) {
          leaf_node = ss.sql("select distinct is_vote_helpful from dataset where 1==1 " + new_where_condition)
          break //continue
        }
        process_data_set(processed_attrs, data, played_for_attr, notplayed_for_attr, new_where_condition)
        if (attr_name_info_gain.isEmpty) {
          leaf_node = ss.sql("select distinct is_vote_helpful from dataset where 1==1 " + new_where_condition)
          break //continue
        }
        // get the attr with max info gain under aValueForMaxGainAttr
        // sort by info gain
        val sorted_by_info_gain = mutable.ListMap(attr_name_info_gain.toSeq.sortBy(_._2): _*)
        var (new_max_gain_attr, new_max_gain_val) = sorted_by_info_gain.head
        if (new_max_gain_val == 0) {
          // under this where condition, records dont have entropy
          leaf_node = ss.sql("select is_vote_helpful distinct from dataset where 1==1 " + new_where_condition)
          break // continue
        }
        val processed_attrs_new = new_max_gain_attr :: processed_attrs // append
        build_tree(new_max_gain_attr, processed_attrs_new, data, new_where_condition)
      }
    })

  }

  def main(args: Array[String]): Unit = {
    val attrs = ("marketplace", "verified_purchase", "star_rating", "vine", "product_category", "review_body")
    // SQLContext is a class and is used for initializing the functionalities of Spark SQL
    println("Here is where the magic begins")

    val dataFrame = ss.read
      .option("delimiter", "\t")
      .option("header", "true")
      .csv("/data/amazon-reduced/")
//      .csv("./data/smaller.tsv")
      .select("marketplace", "verified_purchase", "star_rating", "vine", "product_category", "total_votes", "helpful_votes")
    //      .select("marketplace", "verified_purchase", "star_rating", "vine", "product_category", "review_body", "total_votes", "helpful_votes")

    val n_data_frame = dataFrame
      .withColumn("is_vote_helpful", (dataFrame("helpful_votes") / dataFrame("total_votes")) > threshold)
      .drop("total_votes")
      .drop("helpful_votes")

    val s = n_data_frame.na.fill(false).persist()

    s.printSchema()
    s.select("is_vote_helpful").show()


    s.createOrReplaceTempView("dataset")
    val helpful = ss.sql("SELECT * FROM dataset where is_vote_helpful=true").count()
    val notHelpful = ss.sql("SELECT * FROM dataset WHERE is_vote_helpful=false").count()
    //    val sorted_by_info_gain = sorted(attr_name_info_gain.items(), key=operator.itemgetter(1), reverse=True)
    process_data_set(List[String](), s, helpful, notHelpful, "")
    val sorted_by_info_gain = mutable.ListMap(attr_name_info_gain.toSeq.sortBy(_._2): _*)
    var processed_attrs = List[String]()
    val (max_gain_attr, max_gain_val) = sorted_by_info_gain.head
    processed_attrs = max_gain_attr :: processed_attrs // append
    var where_cond: String = ""
    build_tree(max_gain_attr, processed_attrs, s, where_cond)

    println("Finish")

    //    System.in.read()

  }

}

