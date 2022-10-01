package exercise16

import scopt._

case class Config16(
                   path: String = "",
                   columns: Seq[String] = Seq()
                 )
object Config16 {
  val parser = new OptionParser[Config16]("prognameEx16") {
    head("Ex16")

    opt[String]("path") required() action {
      (v, c) => c.copy(path = v)
    } text "The path of a CSV data set to load"

    opt[Seq[String]]("col") required() action {
      (v, c) => c.copy(columns = v)
    } text "One or more column names"


    note("Notice")
    help("help") text "prints usage text"

    override def errorOnUnknownArgument = false

    override def showUsageOnError = true
  }

  def parseArgs(args: Array[String]): Config16 = {
    parser.parse(args, Config16()) match {
      case Some(conf) => conf
      case None => sys.error("Could not parse arguments")
    }
  }

}
