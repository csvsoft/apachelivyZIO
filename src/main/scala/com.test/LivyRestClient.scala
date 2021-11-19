package com.test

import com.test.entity.{AppInfo, SQLOutput, Session, SessionsResult, SparkField, SparkSchema, StatementReturnData, TextOutput}
import sttp.client.HttpURLConnectionBackend
import sttp.client._
import sttp.client.asynchttpclient.zio.SttpClient
import sttp.client.circe._
import zio._
import zio.duration._
import zio.console._
import com.test.entity._
import io.circe.Decoder.Result
import io.circe._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.generic.auto._


class LivyRestClient(uri: String) {

  //implicit val backend = HttpURLConnectionBackend()
  implicit val appInfoDecoder: Decoder[AppInfo] = deriveDecoder[AppInfo]
  implicit val sessionDecoder: Decoder[Session] = deriveDecoder[Session]
  implicit val sessionResultDecoder: Decoder[SessionsResult] = deriveDecoder[SessionsResult]

  implicit val fieldDecoder: Decoder[SparkField] = deriveDecoder[SparkField]
  implicit val schemaDecoder: Decoder[SparkSchema] = deriveDecoder[SparkSchema]

  implicit val StatementReturnDataDecoder: Decoder[StatementReturnData] = deriveDecoder[StatementReturnData]

  implicit val stmtTextoutDecoder: Decoder[StatementTextOutput] = deriveDecoder[StatementTextOutput]
  implicit val stmtSQLoutDecoder: Decoder[StatementSQLOutput] = deriveDecoder[StatementSQLOutput]
  implicit val stmtPayloadEncoder: Encoder[StatementPayLoad] = deriveEncoder[StatementPayLoad]

  implicit val decodeStatmentResult: Decoder[StatementResult] = new Decoder[StatementResult] {
    override def apply(c: HCursor): Result[StatementResult] = {
      c.as[Statement].map(StatementResult(_, c.value.toString))
    }
  }

  implicit val decodeTextOutput: Decoder[TextOutput] = new Decoder[TextOutput] {
    override def apply(c: HCursor): Result[TextOutput] = {
      c.downField("text/plain").as[String].map(s => TextOutput(s))
    }
  }

  implicit val decodeGeneralStatement: Decoder[GeneralStatement] = new Decoder[GeneralStatement] {
    override def apply(c: HCursor): Result[GeneralStatement] = {
      val cursorOutput = c.downField("output")
      for {
        stmt <- c.as[Statement]
        outputBaseOpt <- cursorOutput.as[Option[StatementOutputBase]]
        textOutOpt <- outputBaseOpt match {
          case Some(b) => decodeStmtTextOutput(cursorOutput, b).map(Some(_))
          case None => Right(None)
        }
      } yield GeneralStatement(stmt, textOutOpt)
    }

    private def decodeStmtTextOutput(cursorOutput: ACursor, outputBase: StatementOutputBase): Result[StatementTextOutput] = {
      if (outputBase.status == "error") {
        for {
          ename <- cursorOutput.downField("ename").as[String]
          evalue <- cursorOutput.downField("evalue").as[String]
          traceback <- cursorOutput.downField("traceback").as[Array[String]]
        } yield StatementErrorTextOutput(outputBase, ename, evalue, traceback)
      } else {
        for {
          textoutput <- cursorOutput.downField("data").as[TextOutput]
        } yield StatementSuccessTextOutput(outputBase, textoutput)

      }
    }
  }
  //SQLStatement
  implicit val decodeSQLStatement: Decoder[SQLStatement] = new Decoder[SQLStatement] {
    override def apply(c: HCursor): Result[SQLStatement] = {
      val cursorOutput = c.downField("output")
      for {
        stmt <- c.as[Statement]
        outputBaseOpt <- cursorOutput.as[Option[StatementOutputBase]]
        stmtSQLOutput <- outputBaseOpt match {
          case Some(b) => decodeStmtSQLOutput(cursorOutput, b).map(Some(_))
          case None => Right(None)
        }
      } yield SQLStatement(stmt, stmtSQLOutput)

    }

    private def decodeStmtSQLOutput(cursorOutput: ACursor, outputBase: StatementOutputBase): Result[StatementSQLOutput] = {
      if (outputBase.status == "error") {
        for {
          ename <- cursorOutput.downField("ename").as[String]
          evalue <- cursorOutput.downField("evalue").as[String]
          traceback <- cursorOutput.downField("traceback").as[Array[String]]
        } yield StatementSQLErrorOutput(outputBase, ename, evalue, traceback)
      } else {
        for {
          sqloutput <- cursorOutput.downField("data").as[SQLOutput]
        } yield StatementSQLSuccessOutput(outputBase, sqloutput)

      }
    }
  }
  implicit val decodeSQLOutput: Decoder[SQLOutput] = new Decoder[SQLOutput] {
    override def apply(c: HCursor): Result[SQLOutput] = {
      val jsonObj = c.downField("application/json")
      for {
        schema <- jsonObj.downField("schema").as[SparkSchema]
        fields = schema.fields
        rowArrayCursor = jsonObj.downField("data").withFocus(j => {
          j.mapArray(v => v.iterator.map(row => {

            row.mapArray(cols => cols.iterator.zipWithIndex.map(fi => {
              val (f, i) = (fi._1, fi._2)
              val sValue = fields(i).`type` match {

                case "string" | "date" | "timestamp" => f.withString(s => Json.fromString(s))
                case "boolean" => f.withBoolean(b => Json.fromString(b.toString))
                case "int" | "byte" | "short" => f.withNumber(jn => Json.fromString(jn.toInt.fold("")(_.toString)))
                case "long" => f.withNumber(jn => Json.fromString(jn.toLong.fold("")(_.toString)))
                case "float" => f.withNumber(jn => Json.fromString(jn.toFloat.toString))
                case "double" => f.withNumber(jn => Json.fromString(jn.toDouble.toString))
                case "decimal" => f.withNumber(jn => Json.fromString(jn.toBigDecimal.fold("")(_.toString)))
                case o => throw new RuntimeException(s"Not supported sql data type:$o to deserialize sql output")
              }
              sValue
            }).toVector)
          }).toVector)
        })
        rowArray <- rowArrayCursor.as[Array[Array[String]]]
      } yield SQLOutput(schema, rowArray)

    }
  }


  def listSessionsZIO(): ZIO[LivyR, Throwable, SessionsResult] = {
    val request = basicRequest
      .get(uri"$uri/sessions")
      .response(asJson[SessionsResult])
    SttpClient.send(request).flatMap(r => ZIO.fromEither(convertResponse(r)))
  }

  private def convertResponse[T](res: Response[Either[ResponseError[Error], T]]): Either[Throwable, T] = {
    res.body match {
      case Right(t) => Right(t)
      case Left(err) => {
        Left(new RuntimeException(err.getMessage + err.body, err.getCause))
      }
    }
  }

  def createSession(name: String): ZIO[LivyR, Throwable, Session] = {
    val request = basicRequest
      .body(s"""{ "name": "$name" }""")
      .response(asJson[Session])
      .post(uri"$uri/sessions")
    sendRequest(request)
  }

  def getIdleSessions(): ZIO[LivyR, Throwable, Session] = {
    listSessionsZIO().map(sr => sr.sessions.filter(_.state == "idle"))
      .flatMap(idlSessions => {
        if (idlSessions.isEmpty) {
          ZIO.fail(new RuntimeException("No idle session available"))
        } else {
          ZIO.succeed(idlSessions.head)
        }
      })
  }

  def getSessionLogs(sessionId: Int, from: Int, size: Int): ZIO[LivyR, Throwable, SessionLog] = {
    val logURI = uri"$uri/sessions/$sessionId/log?from=$from&size=$size"
    sendRequest(basicRequest.response(asJson[SessionLog])
      .get(logURI))
  }

  def getOrCreateSession(name: String): ZIO[LivyR, Throwable, Session] = {
    listSessionsZIO().flatMap(sr => {
      val idleSessions = sr.sessions.filter(_.state == "idle")
      if (idleSessions.isEmpty) {
        createSession(name).flatMap(_ => getIdleSessions().retry(schedule))
      } else {
        ZIO.succeed(idleSessions.head)
      }
    })
  }

  def schedule[A] = Schedule.recurs(20) && Schedule.spaced(1.second).onDecision((a: A, s) => s match {
    case None => putStrLn(s"done trying")
    case Some(att) => putStrLn(s"attempt #$att")
  })

  private def sendRequest[T](request: Request[Either[ResponseError[Error], T], Nothing]): ZIO[LivyR, Throwable, T] = {
    SttpClient.send(request).flatMap(r => ZIO.fromEither(convertResponse(r)))
  }

  private def stmtSchedule[T <: HasStatementState]: Schedule[Console with zio.clock.Clock, T, T] = {
    val s1 = (Schedule.recurs(10) && Schedule.spaced(2.second)) && Schedule.doWhile((s: T) => (s.getState == "waiting" || s.getState
      == "running"))
    val schedule = s1.onDecision((a: T, s) => s match {
      case None => putStrLn(s"Statement get completed.")
      case Some(att) => putStrLn(s"Check statements attempt #$att,statement status is: ${a.getState} ")
    }).map(_._2)
    schedule
  }

  def getSQLStatementUntilCompleted(sessionId: Int, statementId: Int): ZIO[LivyR, Throwable, SQLStatement] = {
    getSQLStatementStatus(sessionId, statementId).repeat(stmtSchedule)
  }

  def getSQLStatementStatus(sessionId: Int, statementId: Int): ZIO[LivyR, Throwable, SQLStatement] = {
    val stmturi = uri"$uri/sessions/$sessionId/statements/$statementId"
    val request = basicRequest.response(asJson[SQLStatement]).get(stmturi)
    sendRequest(request)
  }

  def getScalaStatementUntilCompleted(sessionId: Int, statementId: Int): ZIO[LivyR, Throwable, GeneralStatement] = {
    val stmturi = uri"$uri/sessions/$sessionId/statements/$statementId"
    val request = basicRequest.response(asJson[GeneralStatement]).get(stmturi)
    sendRequest(request).repeat(stmtSchedule)
  }

  def getStatementUntilCompleted(sessionId: Int, statementId: Int, codeKind: String): ZIO[LivyR, Throwable, StatementResult] = {
    val stmturi = uri"$uri/sessions/$sessionId/statements/$statementId"
    val request = basicRequest.response(asJson[StatementResult]).get(stmturi)
    sendRequest(request).repeat(stmtResultSchedule)
  }

  def runStatementCompleted(sessionId: Int, statement: StatementPayLoad): ZIO[LivyR, Throwable, StatementResult] = {
    for {
      stmtR <- runStatement(sessionId, statement)
      stmtResult <- getStatementUntilCompleted(sessionId, stmtR.stmt.id, statement.kind)
    } yield stmtResult
  }

  def stmtResultSchedule = {
    val s1 = (Schedule.recurs(10) && Schedule.spaced(2.second)) && Schedule.doWhile((s: StatementResult) => (s.stmt.state == "waiting" || s.stmt.state == "running"))
    s1.onDecision((a: StatementResult, s) => s match {
      case None => putStrLn(s"done trying")
      case Some(att) => putStrLn(s"Check statements attempt #$att,statement status is: ${a.stmt.state} ")
    }).map(_._2)
  }

  def runSQLStatement(sessionId: Int, sql: String): ZIO[LivyR, Throwable, SQLStatement] = {
    val stmtPayload = StatementPayLoad(sql, CodeKind.SQL)
    val stmturi = uri"$uri/sessions/$sessionId/statements"
    val request = basicRequest.body(stmtPayload)
      .contentType("application/json")
      .response(asJson[SQLStatement])
      .post(stmturi)
    sendRequest(request)
  }

  def runSQLStatementUntilCompleted(sessionId: Int, sql: String): ZIO[LivyR, Throwable, SQLStatement] = {
    runSQLStatement(sessionId, sql)
      .flatMap(stmt => getSQLStatementUntilCompleted(sessionId, stmt.stmt.id))
      .flatMap(stmt => stmt.output match {
        case Some(e: StatementSQLErrorOutput) => ZIO.fail(new RuntimeException(s"Failed to execute SQL:${stmt.stmt.code}\n error: ${e.ename}-${e.evalue}"))
        case _ => ZIO.succeed(stmt)
      })
  }

  def runScalaStatement(sessionId: Int, scalaCode: String): ZIO[LivyR, Throwable, GeneralStatement] = {
    val stmtPayload = StatementPayLoad(scalaCode, CodeKind.SPARK)
    val stmturi = uri"$uri/sessions/$sessionId/statements"
    val request = basicRequest.body(stmtPayload)
      .contentType("application/json")
      .response(asJson[GeneralStatement])
      .post(stmturi)
    sendRequest(request)
  }

  def runScalaStatementUntilComplete(sessionId: Int, scalaCode: String): ZIO[LivyR, Throwable, GeneralStatement] = {
    runScalaStatement(sessionId, scalaCode)
      .flatMap(stmt => getScalaStatementUntilCompleted(sessionId, stmt.stmt.id))
      .flatMap(stmt => stmt.output match {
        case Some(e: StatementErrorTextOutput) => ZIO.fail(new RuntimeException(s"Failed to execute scala code:${stmt.stmt.code}\n error: ${e.ename}:${e.evalue}"))
        case _ => ZIO.succeed(stmt)
      })
  }

  ///sessions/{sessionId}/completion
  def getCodeCompletion(sessionId: Int, code: String, codeKind: String, cursor: Int): ZIO[LivyR, Throwable, CodeCandidates] = {
    val stmturi = uri"$uri/sessions/$sessionId/completion"
    val request = basicRequest.body(CodeCompletionRequest(code, codeKind, cursor))
      .contentType("application/json")
      .response(asJson[CodeCandidates])
      .post(stmturi)
    sendRequest(request)
  }

  def runStatement(sessionId: Int, statement: StatementPayLoad): ZIO[LivyR, Throwable, StatementResult] = {
    /* import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
    val asStatementResult =
      asString.map(ResponseAs.deserializeRightWithError(s => decode(s).map(stmt => StatementResult(stmt, s))))
   */
    val stmturi = uri"$uri/sessions/$sessionId/statements"
    val request = basicRequest.body(statement)
      .contentType("application/json")
      .response(asJson[StatementResult])
      .post(stmturi)
    sendRequest(request)
  }

}

object LivyRestClient {


}