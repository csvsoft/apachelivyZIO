package com.test

import java.util.UUID

import com.test.entity.{CodeKind, GeneralStatement, SQLOutput, SessionsResult, SparkField, SparkSchema, StatementPayLoad, StatementReturnData, TextOutput}
import io.circe.Decoder.Result
import io.circe.{Decoder, HCursor, Json}
import org.junit.Assert
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers
import shapeless.HList
import sttp.client.asynchttpclient.zio.{AsyncHttpClientZioBackend, SttpClient}
import zio.{Runtime, ZIO}

import scala.concurrent.Future

class LivyRestClientTest extends AsyncFunSuite with Matchers {

  test("list sessions with ZIO") {
    Future {
      val client = new LivyRestClient("http://localhost:8998")
      val allSessions = client.listSessionsZIO()
      val sessionsResult = runZIOSttp(allSessions)
      println(sessionsResult)
      assert(true)
    }
  }

  private def runZIOSttp[T](request: ZIO[LivyR, Throwable, T]): T = {
    val runtime = Runtime.default
    val res = request.provideCustomLayer(AsyncHttpClientZioBackend.layer())
    runtime.unsafeRun(res)
  }

  private def getLoadCSVCode(): String = {
    val code =
      """ val df = spark.sqlContext.read.option("header",true).csv("/Users/zrq/Downloads/WIKI-AAL.csv")
        |  df.createOrReplaceTempView("AAL")
        |""".stripMargin
    code
  }

  test("test code completion") {
    Future {
      val client = new LivyRestClient("http://localhost:8998")
      val runStatement = for {

        session <- client.getOrCreateSession("test1-" + UUID.randomUUID().toString)

        loadCSV <- client.runScalaStatementUntilComplete(session.id, getLoadCSVCode())
        candidates <- client.getCodeCompletion(session.id,"df.a",CodeKind.SPARK,4)

        sqlcandidates <- client.getCodeCompletion(session.id,"select * from aal a",CodeKind.SQL,"select * from a".length)

        initLogs <- client.getSessionLogs(session.id, 0, 1)
        sessionLogs <- client.getSessionLogs(session.id, 0, initLogs.total)
      } yield (sqlcandidates, sessionLogs)

      val result = runZIOSttp(runStatement)
      val (queryResult, sessionLog) = (result._1, result._2)
      println(queryResult)
      sessionLog.log.getOrElse(Array[String]()).foreach(println)
      assert(true)
    }
  }

  test("test run scala SQL statement") {
    Future {
      val client = new LivyRestClient("http://localhost:8998")
      val runStatement = for {

        session <- client.getOrCreateSession("test1-" + UUID.randomUUID().toString)

        loadCSV <- client.runScalaStatementUntilComplete(session.id, getLoadCSVCode())
        //invalidScala <- client.runScalaStatementUntilComplete(session.id, "val df = spark.")

       // query <- client.runSQLStatementUntilCompleted(session.id, "select * from AAL limit 10x")
        tables <- client.runSQLStatementUntilCompleted(session.id, "show tables")

        initLogs <- client.getSessionLogs(session.id, 0, 1)
        sessionLogs <- client.getSessionLogs(session.id, 0, initLogs.total)
      } yield (tables, sessionLogs)

      val result = runZIOSttp(runStatement)
      val (queryResult, sessionLog) = (result._1, result._2)
      println(queryResult)
      sessionLog.log.getOrElse(Array[String]()).foreach(println)
      assert(true)
    }
  }

  test("test run statement completed") {
    Future {
      val client = new LivyRestClient("http://localhost:8998")
      val runStatement = for {
        session <- client.getOrCreateSession("test1-" + UUID.randomUUID().toString)

        loadCSV <- client.runStatement(session.id, StatementPayLoad(getLoadCSVCode(), CodeKind.SPARK))
        loadCSVResult <- client.getStatementUntilCompleted(session.id, loadCSV.stmt.id, CodeKind.SPARK)
        query <- client.runStatement(session.id, StatementPayLoad("select * from AAL limit 10", "sql"))
        queryResult <- client.getStatementUntilCompleted(session.id, query.stmt.id, CodeKind.SQL)
        tables <- client.runStatementCompleted(session.id, StatementPayLoad("show tables", CodeKind.SQL))
        initLogs <- client.getSessionLogs(session.id, 0, 1)
        sessionLogs <- client.getSessionLogs(session.id, 0, initLogs.total)
      } yield (tables, sessionLogs)

      val result = runZIOSttp(runStatement)
      val (queryResult, sessionLog) = (result._1, result._2)
      println(queryResult)
      sessionLog.log.getOrElse(Array[String]()).foreach(println)
      assert(true)
    }
  }
  test("decode HList test") {
    val payload =
      """
        |{
        |  "id" : 11,
        |  "code" : "show tables",
        |  "state" : "available",
        |  "output" : {
        |    "status" : "ok",
        |    "execution_count" : 11,
        |    "data" : {
        |      "application/json" : {
        |        "schema" : {
        |          "type" : "struct",
        |          "fields" : [
        |            {
        |              "name" : "database",
        |              "type" : "string",
        |              "nullable" : false,
        |              "metadata" : {
        |
        |              }
        |            },
        |            {
        |              "name" : "tableName",
        |              "type" : "string",
        |              "nullable" : false,
        |              "metadata" : {
        |
        |              }
        |            },
        |            {
        |              "name" : "isTemporary",
        |              "type" : "boolean",
        |              "nullable" : false,
        |              "metadata" : {
        |
        |              }
        |            }
        |          ]
        |        },
        |        "data" : [
        |          [
        |            "",
        |            "aal",
        |            true
        |          ]
        |        ]
        |      }
        |    }
        |  },
        |  "progress" : 1.0,
        |  "started" : 1592060983074,
        |  "completed" : 1592060983086
        |}
        |""".stripMargin

    import org.json4s._
    import org.json4s.native.JsonMethods._
    implicit val formats = DefaultFormats

    val jsonObj = parse(payload)
    val dataJobj = jsonObj \ "output" \ "data" \ "application/json" \ "data"

    val dataScala = dataJobj match {
      case a:JArray => a.arr.map (row =>{
        row match {
          case col:JArray => col.arr.map(jValue=>{
            jValue match {
              case JNull => ""
              case s:JString => s.values
              case b:JBool => b.value
              case c:JInt => c.values
              case l:JLong => l.values
              case d:JDouble => d.values
            }
          })
        }
      })
      case o=> throw new RuntimeException("Not valid data format")
    }
    dataScala.foreach(row=> println(row.mkString(",")))



    Future {

      assert(true)
    }

  }

  private def decodeStatement(stmtJson: String): Unit = {
    import org.json4s._
    import org.json4s.native.JsonMethods._
    implicit val formats = DefaultFormats

    val jsonObj = parse(stmtJson)
    val outputJson = jsonObj \ "output" \ "data" \ "application/json"
    val outputText = jsonObj \ "output" \ "data" \ "text/plain"

    val statementReturnData: Option[StatementReturnData] = if (outputJson != JNothing) {
      val sqloutput = outputJson.extract[SQLOutput]
      Some(sqloutput)
    } else if (outputText != JNothing) {
      Some(TextOutput(outputText.extract[String]))
    } else {
      None
    }


  }

  test("circe decode sql output") {
    val sqlOutput =
      """
{
	"id": 11,
	"code": "select * from AAL limit 10",
	"state": "available",
  "myfield" : null
	"output": {
		"status": "ok",
		"execution_count": 11,
		"data": {
			"application/json": {
				"schema": {
					"type": "struct",
					"fields": [{
						"name": "Date",
						"type": "string",
						"nullable": true,
						"metadata": {}
					}, {
						"name": "Open",
						"type": "string",
						"nullable": true,
						"metadata": {}
					}, {
						"name": "High",
						"type": "string",
						"nullable": true,
						"metadata": {}
					}, {
						"name": "Low",
						"type": "string",
						"nullable": true,
						"metadata": {}
					}, {
						"name": "Close",
						"type": "string",
						"nullable": true,
						"metadata": {}
					}, {
						"name": "Volume",
						"type": "string",
						"nullable": true,
						"metadata": {}
					}, {
						"name": "Ex-Dividend",
						"type": "string",
						"nullable": true,
						"metadata": {}
					}, {
						"name": "Split Ratio",
						"type": "string",
						"nullable": true,
						"metadata": {}
					}, {
						"name": "Adj. Open",
						"type": "string",
						"nullable": true,
						"metadata": {}
					}, {
						"name": "Adj. High",
						"type": "string",
						"nullable": true,
						"metadata": {}
					}, {
						"name": "Adj. Low",
						"type": "string",
						"nullable": true,
						"metadata": {}
					}, {
						"name": "Adj. Close",
						"type": "string",
						"nullable": true,
						"metadata": {}
					}, {
						"name": "Adj. Volume",
						"type": "string",
						"nullable": true,
						"metadata": {}
					}]
				},
				"data": [
					["2018-03-27", "52.3", "52.35", "50.55", "50.9", "2805331.0", "0.0", "1.0", "52.3", "52.35", "50.55", "50.9", "2805331.0"],
					["2018-03-26", "51.94", "52.04", "50.64", "51.86", "3185475.0", "0.0", "1.0", "51.94", "52.04", "50.64", "51.86", "3185475.0"],
					["2018-03-23", "52.63", "53.1", "50.99", "51.01", "3843474.0", "0.0", "1.0", "52.63", "53.1", "50.99", "51.01", "3843474.0"],
					["2018-03-22", "53.58", "54.18", "52.29", "52.31", "4820379.0", "0.0", "1.0", "53.58", "54.18", "52.29", "52.31", "4820379.0"],
					["2018-03-21", "54.21", "54.61", "53.165", "54.09", "4785030.0", "0.0", "1.0", "54.21", "54.61", "53.165", "54.09", "4785030.0"],
					["2018-03-20", "54.93", "55.7", "54.67", "55.32", "2497070.0", "0.0", "1.0", "54.93", "55.7", "54.67", "55.32", "2497070.0"],
					["2018-03-19", "55.33", "55.69", "54.19", "54.62", "3743641.0", "0.0", "1.0", "55.33", "55.69", "54.19", "54.62", "3743641.0"],
					["2018-03-16", "55.31", "56.41", "55.27", "55.4", "4455977.0", "0.0", "1.0", "55.31", "56.41", "55.27", "55.4", "4455977.0"],
					["2018-03-15", "55.35", "55.56", "54.93", "55.17", "2964323.0", "0.0", "1.0", "55.35", "55.56", "54.93", "55.17", "2964323.0"],
					["2018-03-14", "56.57", "57.21", "55.16", "55.28", "4262349.0", "0.0", "1.0", "56.57", "57.21", "55.16", "55.28", "4262349.0"]
				]
			}
		}
	},
	"progress": 1.0,
	"started": 1591754692286,
	"completed": 1591754692664
} """.stripMargin
    import org.json4s._
    import org.json4s.native.JsonMethods._
    //import org.json4s.jackson.JsonMethods._
    val jsValue = parse(""" { "numbers" : [1, 2, 3, 4] } """, false, false)
    val testJson = """{"name":"John",age:11}"""
    val jobj = parse(sqlOutput)
    implicit val formats = DefaultFormats

    val outputJson = jobj \ "output" \ "data" \ "application/json"
    if (outputJson != JNothing) {
      val sqloutput = outputJson.extract[SQLOutput]
      println(sqloutput.schema)
    }
    val dataNone = jobj \ "outputx"
    val nullValue = jobj \ "myfield" \ "subfield"
    if (dataNone == JNothing) {
      println("Has no such field")
    }


    val stmt = decodeStatement(sqlOutput)

    //BaseStatement

    Future {
      assert(true)
    }
  }


  test("circe decode") {
    val payload =
      """
        |{"from":0,"total":1,"sessions":[{"id":6,"name":"test1","appId":null,"owner":null,"proxyUser":null,"state":"dead","kind":"shared","appInfo":{"driverLogUrl":null,"sparkUiUrl":null},"log":["\tat org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)","Caused by: io.netty.channel.ConnectTimeoutException: connection timed out: /fe80:0:0:0:a44a:eff:fee2:6f96%9:10000","\tat io.netty.channel.nio.AbstractNioChannel$AbstractNioUnsafe$1.run(AbstractNioChannel.java:267)","\tat io.netty.util.concurrent.PromiseTask$RunnableAdapter.call(PromiseTask.java:38)","\tat io.netty.util.concurrent.ScheduledFutureTask.run(ScheduledFutureTask.java:120)","\tat io.netty.util.concurrent.AbstractEventExecutor.safeExecute(AbstractEventExecutor.java:163)","\tat io.netty.util.concurrent.SingleThreadEventExecutor.runAllTasks(SingleThreadEventExecutor.java:403)","\tat io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:463)","\tat io.netty.util.concurrent.SingleThreadEventExecutor$5.run(SingleThreadEventExecutor.java:858)","\tat java.lang.Thread.run(Thread.java:745)"]}]}
        |""".stripMargin
    import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
    val sessionResult = decode[SessionsResult](payload)
    sessionResult match {
      case Left(error) => error.printStackTrace()
      case Right(s) => {
        val from = s.from
        println(s)
      }
    }
    Future {
      assert(true)
    }

  }
}
