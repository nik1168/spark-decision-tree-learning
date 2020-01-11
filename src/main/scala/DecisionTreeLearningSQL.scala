import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf,avg}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.util.control.Breaks._
import scala.math.pow

/**
 * Decision tree learning algorithm (ID3) using spark, implementation based on the following blog: https://towardsdatascience.com/machine-learning-decision-tree-using-spark-for-layman-8eca054c8843
 * The main approach of this implementation is
 */
object DecisionTreeLearningSQL {

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
   *
   * @param entropy   : Entropy of parent set
   * @param dataFrame : Contains the different values that a variable can take and the number of helpful and not helpful votes
   * @param countTotalElements
   * @return information gain IG(S,A)
   */
  def getInformationGain(entropy: Double, dataFrame: DataFrame, countTotalElements: Long): Double = {
    var averageWeightedAttributeEntropy: Double = 0.0 // Average weighted entropy
    // Notice that we are looping over the distinct values of an attribute which can fit into the memory of the main node
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
   *
   * @param attr               : Attribute
   * @param data               : Data frame
   * @param entropy            : Parent's entropy
   * @param countTotalElements : Total elements
   * @param condition          : Condition for where clause
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
   *
   * @param excludedAttrs : Attributes that were already processed
   * @param data          : Dataframe
   * @param helpful       : Number of helpful reviews
   * @param notHelpful    : Number of not helpful reviews
   * @param condition     : condition for where clause
   */
  def processData(excludedAttrs: List[String], data: DataFrame, helpful: Long, notHelpful: Long, condition: String): Unit = {
    val countTotalElements: Long = helpful + notHelpful // Total number of elements
    val subsInfo = Map[String, Long]("helpful" -> helpful, "notHelpful" -> notHelpful) // Dictionary with number of helpful and not helpful reviews
    val entropy = calculateEntropy(countTotalElements, subsInfo) // Entropy of parent set
    println("Condition: ", condition)
    println("The entropy is: ", entropy)
    attributeNameInfoGain = collection.mutable.Map[String, Double]() // Declare dictionary to store the info gain of the attributes for the present set
    println("Already processed attributes: ", excludedAttrs)
    val attrs = List("marketplace", "verified_purchase", "vine", "product_category", "review_body") // Chosen attributes for feature selection
    attrs.foreach(attr => {
      if (!excludedAttrs.contains(attr)) {
        getAttributesInformationGain(attr, data, entropy, countTotalElements, condition) // Fill info gain dictionary with each attribute
      }
    })
  }


  /**
   * Return the log base 2 of a number
   *
   * @param x : number of type Double
   * @return log2(number)
   */
  def log2Val(x: Double): Double = {
    val lnOf2 = scala.math.log(2)
    scala.math.log(x) / lnOf2
  }

  /**
   * Gets the entropy of a set H(S)
   *
   * @param countTotalElements  : Total number of elements
   * @param elementsInEachClass : Total number of elements of each class (helpful reviews, not helpful reviews)
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

  def ID3(attributeMaxInfoGain: String, processedAttributes: List[String], data: DataFrame, condition: String, tree: TreeDecision): Unit = {
    //    println("Tree papaya")
    println("Node: ", attributeMaxInfoGain)
    val attrValues = ss.sql("SELECT distinct " + attributeMaxInfoGain + " FROM dataset  where 1==1 " + condition) // Select distinct values of the root node
    var originCondition = condition // Store original condition to aggregate with following conditions
    // Get each one of the different values the attribute can take
    attrValues.rdd.collect().foreach(aValueForMaxGainAttr => {
      breakable {
        var leaf = ss.emptyDataFrame // used for storing a leaf node
        val adistinctValueForAttribute = aValueForMaxGainAttr(0) // value of distinct value
        // Add edges
        //        G.add_edges_from([(max_gain_attr, adistinctValueForAttribute)])
        val children: TreeDecision = new TreeDecision("", "", List[TreeDecision]())
        println("Add edge from " + attributeMaxInfoGain + " to " + adistinctValueForAttribute + "")
        val newCondition = originCondition + " and " + attributeMaxInfoGain + "=='" + adistinctValueForAttribute + "'" // Split the set, get all the values from the distinct value of the attribute
        val helpfulReviewForAttribute = ss.sql("select * from dataset where is_vote_helpful==true" + newCondition).count() // Helpful reviews of split set
        val notHelpfulReviewForAttribute = ss.sql("select * from dataset where is_vote_helpful==false" + newCondition).count() // Not helpful reviews of split data set
        //        var leaf_values = List[String]()
        if (helpfulReviewForAttribute == 0 || notHelpfulReviewForAttribute == 0) { // If either one of them is 0, then we have a leaf node, which means that if the path goes through that distinct value then
          leaf = ss.sql("select distinct is_vote_helpful from dataset where 1==1 " + newCondition)
          var e: String = ""
          // Notice that the use of collect here is possible since this is a way smaller version of the data set
          leaf.rdd.collect().foreach(leaf_node_data => {
            println("Leaf found due to either helpful reviews or not helpful reviews number is 0")
            println("Add edge from " + attributeMaxInfoGain + " to " + leaf_node_data + "")
            e = leaf_node_data(0).toString
            // G.add_edges_from([(adistinctValueForAttribute, str(leaf_node_data[0]))])
          })
          children.value1 = adistinctValueForAttribute.toString
          children.value2 = e
          tree.children = children :: tree.children
          println("-----------------------")
          break //continue
        }
        processData(processedAttributes, data, helpfulReviewForAttribute, notHelpfulReviewForAttribute, newCondition)
        if (attributeNameInfoGain.isEmpty) {
          var e: String = ""
          leaf = ss.sql("select distinct is_vote_helpful from dataset where 1==1 " + newCondition)
          // Notice that the use of collect here is possible since this is a way smaller version of the data set
          leaf.rdd.collect().foreach(leaf_node_data => {
            println("Leaf found since we processed all attributes")
            println("Add edge from " + attributeMaxInfoGain + " to " + leaf_node_data(0) + "")
            e = leaf_node_data(0).toString
            // G.add_edges_from([(adistinctValueForAttribute, str(leaf_node_data[0]))])
          })
          println("-----------------------")
          children.value1 = adistinctValueForAttribute.toString
          children.value2 = e
          tree.children = children :: tree.children
          break //continue
        }
        // get the attr with max info gain under aValueForMaxGainAttr
        // sort by info gain
        val sortedDescInformationGain = mutable.ListMap(attributeNameInfoGain.toSeq.sortBy(_._2): _*)
        val (newMaxInformationGainAttribute, new_max_gain_val) = sortedDescInformationGain.head
        if (new_max_gain_val == 0) {
          // under this where condition, records dont have entropy
          leaf = ss.sql("select is_vote_helpful distinct from dataset where 1==1 " + newCondition)
          var e: String = ""
          // Notice that the use of collect here is possible since this is a way smaller version of the data set
          leaf.rdd.collect().foreach(leaf_node_data => {
            println("Leaf found since information gain is 0")
            println("Add edge from " + attributeMaxInfoGain + " to " + leaf_node_data(0) + "")
            e = leaf_node_data(0).toString

            // G.add_edges_from([(adistinctValueForAttribute, str(leaf_node_data[0]))])
          })
          println("-----------------------")
          children.value1 = adistinctValueForAttribute.toString
          children.value2 = e
          tree.children = children :: tree.children
          break // continue
        }
        println("Add edge from " + adistinctValueForAttribute + " to " + newMaxInformationGainAttribute + "")
        children.value1 = adistinctValueForAttribute.toString
        children.value2 = newMaxInformationGainAttribute
        tree.children = children :: tree.children
        val newProcessedAttributes = newMaxInformationGainAttribute :: processedAttributes // append
        println("-----------------------")
        ID3(newMaxInformationGainAttribute, newProcessedAttributes, data, newCondition, children)
      }
    })

  }

  case class Tree[+T](value: T, left: Option[Tree[T]], right: Option[Tree[T]])

  class Node() {
    var value1: String = ""
    var value2: String = ""
    var next: Node = null
  }

  class TreeDecision(var value1: String, var value2: String = "", var children: List[TreeDecision]) extends java.io.Serializable {

  }

  def parseReview(review: Row, tree: Long): String = {
    println(review)
    return review.toString()
  }

  def main(args: Array[String]): Unit = {
    val attrs = ("marketplace", "verified_purchase", "star_rating", "vine", "product_category", "review_body")
    // SQLContext is a class and is used for initializing the functionalities of Spark SQL
    println("Here is where the magic begins")

    val mapRatings = udf((rating: String) => {
      val response = if (rating.toLong < 2) "bad" else if (rating.toLong == 3) "regular" else "good"
      response
    })
    val mapReviewText = udf((text: String) => {
      text.length.toDouble
    })
    val mapError = udf((groundTruth: Boolean,predicted:Boolean) => {
      val gint = if (groundTruth) 1 else 0
      val pint = if (predicted) 1 else 0
      pow((gint-pint),2)
    })

    val dataFrame = ss.read
      .option("delimiter", "\t")
      .option("header", "true")
      //      .csv("./data/amazon_reviews_us_Musical_Instruments_v1_00.tsv")
      .csv("./data/smaller.tsv")
      .select("marketplace", "verified_purchase", "star_rating", "vine", "product_category", "review_body", "total_votes", "helpful_votes")
    //      .withColumn("review_body", mapReviewText(dataFrame("review_body")))
    //      .select("marketplace", "verified_purchase", "star_rating", "vine", "product_category", "review_body", "total_votes", "helpful_votes")

    val n_data_frame = dataFrame
      .withColumn("is_vote_helpful", (dataFrame("helpful_votes") / dataFrame("total_votes")) > threshold)
      .withColumn("star_rating", mapRatings(dataFrame("star_rating")))
      .withColumn("review_body", mapReviewText(dataFrame("review_body")).cast(DoubleType))
      .drop("total_votes")
      .drop("helpful_votes")
    //      .drop("star_rating")

    val s = n_data_frame.na.fill(n_data_frame.columns.map(_ -> false).toMap)

    val sets = s.randomSplit(Array[Double](0.7, 0.3), 18)
    val training = sets(0)
    val quantilesTraining = training.stat.approxQuantile("review_body", Array(0.25, 0.5, 0.75), 0)
    val mapQuantilesTraining = udf((lengthText: Double) => {
      val response = if (lengthText <= quantilesTraining(0)) "(0,Q1]" else if (lengthText > quantilesTraining(0) && lengthText <= quantilesTraining(1)) "(Q1,Q2]" else if (lengthText > quantilesTraining(1) && lengthText <= quantilesTraining(2)) "(Q2,Q3]" else "Q3"
      response
    })
    val trainingSet = training.withColumn("review_body", mapQuantilesTraining(training("review_body"))).persist()

    val test = sets(1)
    val quantilesTesting = test.stat.approxQuantile("review_body", Array(0.25, 0.5, 0.75), 0)
    val mapQuantilesTesting = udf((lengthText: Double) => {
      val response = if (lengthText <= quantilesTesting(0)) "(0,Q1]" else if (lengthText > quantilesTesting(0) && lengthText <= quantilesTesting(1)) "(Q1,Q2]" else if (lengthText > quantilesTesting(1) && lengthText <= quantilesTesting(2)) "(Q2,Q3]" else "Q3"
      response
    })
    val testingSet = test.withColumn("review_body", mapQuantilesTesting(training("review_body"))).persist()

    trainingSet.printSchema()
    trainingSet.select("is_vote_helpful").show()


    trainingSet.createOrReplaceTempView("dataset")
    val countHelpful = ss.sql("SELECT * FROM dataset where is_vote_helpful=true").count()
    val countNotHelpful = ss.sql("SELECT * FROM dataset WHERE is_vote_helpful=false").count()
    //    val sortedDescInformationGain = sorted(attr_name_info_gain.items(), key=operator.itemgetter(1), reverse=True)
    processData(List[String](), trainingSet, countHelpful, countNotHelpful, "")
    val sortedDescInformationGain = mutable.ListMap(attributeNameInfoGain.toSeq.sortBy(_._2): _*)
    var processedAttributes = List[String]()
    val (maxInformationGainAttribute, maxInformationGainValue) = sortedDescInformationGain.head
    processedAttributes = maxInformationGainAttribute :: processedAttributes // append
    var initialCondition: String = ""
    println("Root node")
    println(maxInformationGainAttribute)
    val finalTree: TreeDecision = new TreeDecision("", maxInformationGainAttribute, List[TreeDecision]())
    ID3(maxInformationGainAttribute, processedAttributes, trainingSet, initialCondition, finalTree)
    val xTest = testingSet.select("marketplace", "verified_purchase", "vine", "product_category", "review_body")
    val yTest = testingSet.select("is_vote_helpful")
    val prediction = predict(xTest, finalTree)
    val df11 = ss.sqlContext.createDataFrame(
      yTest.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index)
      },
      // Create schema for index column
      StructType(yTest.schema.fields :+ StructField("index", LongType, false))
    )


    val df22 = ss.sqlContext.createDataFrame(
      prediction.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index)
      },
      // Create schema for index column
      StructType(prediction.schema.fields :+ StructField("index", LongType, false))
    )

    val error = df11.join(df22, Seq("index")).drop("index").withColumn("error", mapError(col("is_vote_helpful"),col("prediction").cast(BooleanType)))
    val mse = error.select(avg(col("error"))).show()
    //    val e = xTest.rdd.map(row => finalTree.toString).take(5)

    //    val d = xTest.

    println("Finish")

    //    System.in.read()

  }

  def getPrediction(row: Row, tree: TreeDecision): Row = {
    var copyTree: TreeDecision = tree
    val mapAttr: mutable.Map[String, Long] = collection.mutable.Map[String, Long]("marketplace" -> 0, "verified_purchase" -> 1, "vine" -> 2, "product_category" -> 3, "review_body" -> 4)
    while (copyTree.children.nonEmpty) { // If we didn't find a leaf node
      breakable{
        copyTree.children.foreach((child:TreeDecision)=>{
          if (row(mapAttr(copyTree.value2).toInt) == child.value1){
            copyTree = child
            break
          }
        })
      }
    }
    return Row(copyTree.value2)
  }

  def predict(dataframe: DataFrame, trees: TreeDecision) = {

    val e:RDD[Row] = dataframe.rdd.map(row => getPrediction(row, trees))
    val schema = new StructType()
      .add(StructField("prediction", StringType, true))

    val dfWithSchema = ss.createDataFrame(e,schema)
    dfWithSchema
  }

}

