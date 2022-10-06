import org.apache.spark.sql.SparkSession
import java.util.Properties
import ex38config.Config38

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MySuperCode")
      .getOrCreate()

    val conf = Config38.parseArgs(args)
    println(conf.toString)

    val driver = "org.postgresql.Driver"
    //Class.forName(driver)
    val jdbcHostname = conf.hostname
    val jdbcPort = conf.port
    val jdbcDatabase = conf.database
    val jdbcUrl = s"jdbc:postgresql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"
    val jdbcUsername = conf.username
    val jdbcPassword = conf.password
    val table = conf.table

    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", s"${jdbcUsername}")
    connectionProperties.setProperty("password", s"${jdbcPassword}")
    connectionProperties.setProperty("Driver", driver)

    val test_table = spark.read.jdbc(jdbcUrl, table, connectionProperties)
    test_table.show()
  }
}
