import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.util.control.Breaks._

object DecisionTreeLearningSQL {
  type AttributeId = Int
  val threshold = 0.5 // Threshold for the target variable (helpful votes/total votes > threshold)
  var attributeNameInfoGain: mutable.Map[String, Double] = collection.mutable.Map[String, Double]() // Global variable that stores the information gain of each variable
  val conf: SparkConf = new SparkConf() // Spark configuration
    .setAppName("Niklaus decision tree") // Application name
    .setMaster("local[1]") // Acts as a master node with 1 thread

  val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate() // Spark session to use data frames and Spark SQL

  // Disable information logs, for debugging purposes
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  /**
   * Get the information gain IG(S,A)
   * @param entropy   : Entropy of parent set
   * @param dataFrame : Contains the different values that a variable can take and the number of helpful and not helpful votes
   * @param countTotalElements
   * @return information gain IG(S,A)
   */
  def getInformationGain(entropy: Double, dataFrame: DataFrame, countTotalElements: Long): Double = {
    var averageWeightedAttributeEntropy: Double = 0.0 // Average weighted entropy
    dataFrame.rdd.collect().foreach(anAttributeData => {
      val countHelpfulComments = anAttributeData(1).toString.toLong // Number of helpful reviews of attribute of variable
      val countNotHelpfulComments = anAttributeData(2).toString.toLong // Number of not helpful reviews of attribute of variable
      val countTotal = countHelpfulComments + countNotHelpfulComments

      val classmap = Map("helpful" -> countHelpfulComments, "not_helpful" -> countNotHelpfulComments) // Store counts into a map to be used later
      averageWeightedAttributeEntropy = averageWeightedAttributeEntropy + ((countTotal.toDouble / countTotalElements.toDouble) * calculateEntropy(countTotal, classmap)) // Average weighted entropy
    })
    val gain: Double = entropy - averageWeightedAttributeEntropy
    gain
  }

  /**
   * Get the information gain of an attribute and fills a "dictionary" with the information gain of that attribute to compute the max gain afterwards
   * @param attr: Attribute
   * @param data: Data frame
   * @param entropy: Parent's entropy
   * @param countTotalElements: Total elements
   * @param condition: Condition for where clause
   */
  def getAttributesInformationGain(attr: String, data: DataFrame, entropy: Double, countTotalElements: Long, condition: String): Unit = {
    val groupAttributePositive: DataFrame = if (condition.length == 0) data.filter(col("is_vote_helpful") === true).groupBy(attr).agg(Map("is_vote_helpful" -> "count")).withColumnRenamed("count(is_vote_helpful)", "helpful_count") else data.where("is_vote_helpful=true " + condition).groupBy(attr).agg(Map("is_vote_helpful" -> "count")).withColumnRenamed("count(is_vote_helpful)", "helpful_count")
    val groupAttributeNegative: DataFrame = if (condition.length == 0) data.filter(col("is_vote_helpful") === false).groupBy(attr).agg(Map("is_vote_helpful" -> "count")).withColumnRenamed("count(is_vote_helpful)", "not_helpful_count") else data.where("is_vote_helpful=false " + condition).groupBy(attr).agg(Map("is_vote_helpful" -> "count")).withColumnRenamed("count(is_vote_helpful)", "not_helpful_count")
    val joinedDataFrame: DataFrame = groupAttributePositive.join(groupAttributeNegative, Seq(col(groupAttributePositive.columns(0)).toString()), "outer") // Make an outer join of above dataframes to have a single dataframe with the counts of helpful and not helpful reviews
    val sd = joinedDataFrame.na.fill(0, Array[String]("helpful_count", "not_helpful_count")) // Replace null elements or nan elements with 0, this can happen becuase of the outer join
    val joinedDataFrameFinal: DataFrame = sd
      .withColumn("total", sd("helpful_count") + sd("not_helpful_count")) // Add another column with the sum of both columns
    val attributeInformationGain = getInformationGain(entropy, joinedDataFrameFinal, countTotalElements) // Get attribute information gain
    attributeNameInfoGain(attr) = attributeInformationGain // Add information gain to main dictionary
  }

  /**
   * Process current set with selected attributes, gets the information gain for each one of them
   * @param excludedAttrs: Attributes that were already processed
   * @param data: Dataframe
   * @param helpful: Number of helpful reviews
   * @param notHelpful: Number of not helpful reviews
   * @param condition: condition for where clause
   */
  def processData(excludedAttrs: List[String], data: DataFrame, helpful: Long, notHelpful: Long, condition: String): Unit = {
    val countTotalElements: Long = helpful + notHelpful // Total number of elements
    val subsInfo = Map[String, Long]("helpful" -> helpful, "notHelpful" -> notHelpful) // Dictionary with number of helpful and not helpful reviews
    val entropy = calculateEntropy(countTotalElements, subsInfo) // Entropy of parent set
    println("Condition: ", condition)
    println("Set: ")
    data.printSchema()
    println("The entropy is: ", entropy)
    attributeNameInfoGain = collection.mutable.Map[String, Double]() // Declare dictionary to store the info gain of the attributes for the present set

    val attrs = List("marketplace", "verified_purchase", "star_rating", "vine", "product_category") // Chosen attributes for feature selection
    attrs.foreach(attr => {
      if (!excludedAttrs.contains(attr)) {
        getAttributesInformationGain(attr, data, entropy, countTotalElements, condition) // Fill info gain dictionary with each attribute
      }
    })
  }

  /**
   * Return the log base 2 of a number
   * @param x: number of type Double
   * @return log2(number)
   */
  def log2Val(x: Double): Double = {
    val lnOf2 = scala.math.log(2)
    scala.math.log(x) / lnOf2
  }

  /**
   * Gets the entropy of a set H(S)
   * @param countTotalElements: Total number of elements
   * @param elementsInEachClass: Total number of elements of each class (helpful reviews, not helpful reviews)
   * @return H(S)
   */
  def calculateEntropy(countTotalElements: Long, elementsInEachClass: Map[String, Long]): Double = {
    val keysInMap: Set[String] = elementsInEachClass.keySet
    var entropy: Double = 0.0
    keysInMap.foreach((key) => {
      breakable {
        val numberOfElementsInEachClass = elementsInEachClass(key)
        if (numberOfElementsInEachClass == 0) {
          break
        }
        val ratio: Double = numberOfElementsInEachClass.toDouble / countTotalElements.toDouble
        entropy = entropy - (ratio * log2Val(ratio))
      }

    })
    entropy
  }

  def ID3(attributeMaxInfoGain: String, processedAttributes: List[String], data: DataFrame, condition: String): Unit = {
    //    println("Tree papaya")
    println("Node: ", attributeMaxInfoGain)
    val attrValues = ss.sql("SELECT distinct " + attributeMaxInfoGain + " FROM dataset  where 1==1 " + condition)
    var originCondition = condition
    attrValues.rdd.collect().foreach(aValueForMaxGainAttr => {
      breakable {
        var leaf = ss.emptyDataFrame
        val adistinctValueForAttribute = aValueForMaxGainAttr(0)
        // Add edges
        //        G.add_edges_from([(max_gain_attr, adistinctValueForAttribute)])
        println("Add edge from " + attributeMaxInfoGain + " to " + adistinctValueForAttribute + "")
        val newCondition = originCondition + " and " + attributeMaxInfoGain + "=='" + adistinctValueForAttribute + "'"
        val helpfulReviewForAttribute = ss.sql("select * from dataset where is_vote_helpful==true" + newCondition).count()
        val notHelpfulReviewForAttribute = ss.sql("select * from dataset where is_vote_helpful==false" + newCondition).count()
        //        var leaf_values = List[String]()
        if (helpfulReviewForAttribute == 0 || notHelpfulReviewForAttribute == 0) {
          leaf = ss.sql("select distinct is_vote_helpful from dataset where 1==1 " + newCondition)
          leaf.rdd.collect().foreach(leaf_node_data => {

            //            G.add_edges_from([(adistinctValueForAttribute, str(leaf_node_data[0]))])
          })
          break //continue
        }
        processData(processedAttributes, data, helpfulReviewForAttribute, notHelpfulReviewForAttribute, newCondition)
        if (attributeNameInfoGain.isEmpty) {
          leaf = ss.sql("select distinct is_vote_helpful from dataset where 1==1 " + newCondition)
          break //continue
        }
        // get the attr with max info gain under aValueForMaxGainAttr
        // sort by info gain
        val sortedDescInformationGain = mutable.ListMap(attributeNameInfoGain.toSeq.sortBy(_._2): _*)
        var (newMaxInformationGainAttribute, new_max_gain_val) = sortedDescInformationGain.head
        if (new_max_gain_val == 0) {
          // under this where condition, records dont have entropy
          leaf = ss.sql("select is_vote_helpful distinct from dataset where 1==1 " + newCondition)
          break // continue
        }
        val newProcessedAttributes = newMaxInformationGainAttribute :: processedAttributes // append
        ID3(newMaxInformationGainAttribute, newProcessedAttributes, data, newCondition)
      }
    })

  }

  case class Tree[+T](value: T, left: Option[Tree[T]], right: Option[Tree[T]])

  def main(args: Array[String]): Unit = {
    val attrs = ("marketplace", "verified_purchase", "star_rating", "vine", "product_category", "review_body")
    // SQLContext is a class and is used for initializing the functionalities of Spark SQL
    println("Here is where the magic begins")

    val dataFrame = ss.read
      .option("delimiter", "\t")
      .option("header", "true")
      //      .csv("./data/amazon_reviews_us_Musical_Instruments_v1_00.tsv")
      .csv("./data/smaller.tsv")
      .select("marketplace", "verified_purchase", "star_rating", "vine", "product_category", "total_votes", "helpful_votes")
    //      .select("marketplace", "verified_purchase", "star_rating", "vine", "product_category", "review_body", "total_votes", "helpful_votes")

    val n_data_frame = dataFrame
      .withColumn("is_vote_helpful", (dataFrame("helpful_votes") / dataFrame("total_votes")) > threshold)
      .drop("total_votes")
      .drop("helpful_votes")

    val s = n_data_frame.na.fill(n_data_frame.columns.map(_ -> false).toMap).persist()

    s.printSchema()
    s.select("is_vote_helpful").show()


    s.createOrReplaceTempView("dataset")
    val countHelpful = ss.sql("SELECT * FROM dataset where is_vote_helpful=true").count()
    val countNotHelpful = ss.sql("SELECT * FROM dataset WHERE is_vote_helpful=false").count()
    //    val sortedDescInformationGain = sorted(attr_name_info_gain.items(), key=operator.itemgetter(1), reverse=True)
    processData(List[String](), s, countHelpful, countNotHelpful, "")
    val sortedDescInformationGain = mutable.ListMap(attributeNameInfoGain.toSeq.sortBy(_._2): _*)
    var processedAttributes = List[String]()
    val (maxInformationGainAttribute, maxInformationGainValue) = sortedDescInformationGain.head
    processedAttributes = maxInformationGainAttribute :: processedAttributes // append
    var initialCondition: String = ""
    println("Root node")
    println(maxInformationGainAttribute)
    ID3(maxInformationGainAttribute, processedAttributes, s, initialCondition)

    println("Finish")

    //    System.in.read()

  }

}

