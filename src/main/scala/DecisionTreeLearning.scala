import org.apache.spark.{SparkConf, SparkContext}

object DecisionTreeLearning {
  def main(args: Array[String]): Unit = {
    println("Here is where the magic begins")
    val conf = new SparkConf()
      .setAppName("Niklaus decision tree") // Application name
      .setMaster("local[1]") // Acts as a master node with 1 thread
    val sc = new SparkContext(conf)
    println(sc)
  }
}
