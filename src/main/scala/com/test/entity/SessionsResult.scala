package com.test.entity


/**
 * Right({"from":0,"total":1,"sessions":[{"id":5,"name":"test1","appId":null,"owner":null,
 * "proxyUser":null,"state":"idle","kind":"shared","appInfo":{"driverLogUrl":null,"sparkUiUrl":null},
 * "log":["2020-06-07 12:49:09 INFO  NettyBlockTransferService:54 - Server created on ruquans-mbp:49556","2020-06-07 12:49:09 INFO  BlockManager:54 - Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy","2020-06-07 12:49:09 INFO  BlockManagerMaster:54 - Registering BlockManager BlockManagerId(driver, ruquans-mbp, 49556, None)","2020-06-07 12:49:09 INFO  BlockManagerMasterEndpoint:54 - Registering block manager ruquans-mbp:49556 with 366.3 MB RAM, BlockManagerId(driver, ruquans-mbp, 49556, None)","2020-06-07 12:49:09 INFO  BlockManagerMaster:54 - Registered BlockManager BlockManagerId(driver, ruquans-mbp, 49556, None)","2020-06-07 12:49:09 INFO  BlockManager:54 - Initialized BlockManager: BlockManagerId(driver, ruquans-mbp, 49556, None)","2020-06-07 12:49:09 INFO  ContextHandler:781 - Started o.s.j.s.ServletContextHandler@4e8ef9d8{/metrics/json,null,AVAILABLE,@Spark}","2020-06-07 12:49:09 INFO  SparkEntries:55 - Spark context finished initialization in 776ms","2020-06-07 12:49:09 INFO  SparkEntries:86 - Created Spark session.","\nstderr: "]}]})
 */
case class AppInfo(driverLogUrl: Option[String], sparkUiUrl: Option[String])

case class Session(id: Int, name: String, appId: Option[String], owner: Option[String], proxyUser: Option[String]
                   , state: String, kind: String, appInfo: AppInfo, log: Array[String])

case class SessionsResult(from: Int, total: Int, sessions: Array[Session])

case class SparkField(name: String, `type`: String, nullable: Boolean)

case class SparkSchema(`type`: String, fields: Array[SparkField])

case class SessionLogRequest(from: Int, size: Int)

case class SessionLog(id: Int, from: Int, total: Int, log: Option[Array[String]])
