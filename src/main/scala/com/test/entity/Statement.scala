package com.test.entity

case class StatementPayLoad(code: String, kind: String)
object CodeKind{
  val SPARK = "spark"
  val SQL = "sql"
  val PY_SPARK= "pyspark"
}

/**
 * Statement Output
 * Name	Description	Type
 * status	Execution status	string
 * execution_count	A monotonically increasing number	integer
 * data	Statement output	An object mapping a mime type to the result. If the mime type is ``application/json``, the value is a JSON value.
 * {"id":7,"code":" val df = spark.sqlContext.read.option(\"header\",true).csv(\"/Users/zrq/Downloads/WIKI-AAL.csv\")\n  df.createOrReplaceTempView(\"AAL\")\n","state":"available","output":{"status":"ok","execution_count":7,"data":{"text/plain":"df: org.apache.spark.sql.DataFrame = [Date: string, Open: string ... 11 more fields]\n"}},"progress":1.0,"started":1591753604503,"completed":1591753604906}
 */
case class StatementOutputBase(status: String, execution_count: Int)

 class StatementTextOutput()
case class StatementSuccessTextOutput(base:StatementOutputBase, data: TextOutput) extends StatementTextOutput
case class StatementErrorTextOutput(base:StatementOutputBase, ename:String, evalue:String, traceback:Array[String]) extends StatementTextOutput

 class StatementSQLOutput()
case class StatementSQLSuccessOutput(base:StatementOutputBase, data: SQLOutput) extends StatementSQLOutput
case class StatementSQLErrorOutput(base:StatementOutputBase, ename:String, evalue:String, traceback:Array[String]) extends StatementSQLOutput

case class CodeCompletionRequest(code:String,kind:String,cursor:Int)
case class CodeCandidates(candidates:Array[String])
/**
 * A statement represents the result of an execution statement.
 *
 * Name	Description	Type
 * id	The statement id	integer
 * code	The execution code	string
 * state	The execution state	statement state
 * output	The execution output	statement output
 * progress	The execution progress	double
 * started	The start time of statement code	long
 * completed	The complete time of statement code	long
 */
class Statement(val id: Int, val code: String, val state: String, val progress: Double, val started: Long, val completed: Long)

trait HasStatementState{
  def getState:String
}
case class StatementResult(stmt:Statement,json:String)
final case class GeneralStatement(stmt:Statement, output: Option[StatementTextOutput]) extends HasStatementState{
  override def getState: String = stmt.state
}
final case class SQLStatement(stmt:Statement,  output: Option[StatementSQLOutput]) extends HasStatementState{
  override def getState: String = stmt.state
}

sealed trait StatementReturnData

case class SQLOutput(schema: SparkSchema, data: Array[Array[String]]) extends StatementReturnData

case class TextOutput(text: String) extends StatementReturnData
