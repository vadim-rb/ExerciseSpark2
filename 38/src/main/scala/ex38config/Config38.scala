package ex38config

import scopt._

case class Config38(
                     hostname: String="",
                     port:Int=5432,
                     database:String="",
                     username:String="",
                     password:String="",
                     table:String=""
                   )
object Config38 {
  val parser: OptionParser[Config38] = new OptionParser[Config38]("prognameEx38") {
    head("Ex38")

    opt[String]("hostname") required() action {
      (v, c) => c.copy(hostname = v)
    }

    opt[Int]("port") required() action {
      (v, c) => c.copy(port = v)
    }

    opt[String]("database") required() action {
      (v, c) => c.copy(database = v)
    }

    opt[String]("username") required() action {
      (v, c) => c.copy(username = v)
    }

    opt[String]("password") required() action {
      (v, c) => c.copy(password = v)
    }

    opt[String]("table") required() action {
      (v, c) => c.copy(table = v)
    }


    override def errorOnUnknownArgument = false

    override def showUsageOnError = true
  }

  def parseArgs(args: Array[String]): Config38 = {
    parser.parse(args, Config38()) match {
      case Some(conf) => conf
      case None => sys.error("Could not parse arguments")
    }
  }}
