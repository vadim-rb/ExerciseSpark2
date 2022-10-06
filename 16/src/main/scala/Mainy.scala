import exercise16.Config16
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.annotation.tailrec
import scala.language.implicitConversions

//java -cp "scala-library-2.11.8.jar;scopt_2.11-3.5.0.jar;testscopty2_2.11-0.1.0-SNAPSHOT.jar" Mainy --path 'pp' --col a,
/*
/usr/hdp/2.6.1.0-129/spark2/bin/spark-submit --master yarn-cluster --class "Mainy" testscopty2-assembly-0.1.0-SNAPSHOT.jar --path "/user/test/input16.csv" --col city,country
 */

object Mainy {
  def main(args: Array[String]): Unit = {
    val conf = Config16.parseArgs(args)
    println(conf.columns)
    println(conf.path)
    val spark: SparkSession = SparkSession.builder()
      .appName("MySuperCode")
      .getOrCreate()

    val df = spark.read.options(Map("inferSchema"->"true", "header"->"true")).csv(conf.path)
    df.show()
    implicit def bool2int(b:Boolean): Int = if (b) 0 else 1
    val colscsv = df.columns.toList
    val usercolumns = conf.columns
    val check_columns = usercolumns.foldLeft(0)((sum, item) => sum + colscsv.contains(item):Int)
    if (check_columns!=0){
      throw new IllegalArgumentException("User's column are not in CSV's columns")
    }
    val up_ = udf((input: String) => input.toUpperCase)

    @tailrec
    def uppy(df: DataFrame, columns : List[String]):DataFrame = {
      columns match {
        case Nil => df
        case head :: tail => uppy(df.withColumn("upper_"+head,up_(col(head))),tail)
      }
    }

    val resultDF = uppy(df,usercolumns.toList)
    resultDF.show(false)
    
  }
}
