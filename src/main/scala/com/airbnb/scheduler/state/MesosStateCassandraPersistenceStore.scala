package com.airbnb.scheduler.state

import java.util.logging.{Level, Logger}
import scala.collection.mutable
import scala.Some

import com.airbnb.scheduler.config.{CassandraConfiguration, SchedulerConfiguration}
import com.airbnb.scheduler.jobs._
import com.google.inject.Inject
import com.twitter.common.zookeeper.ZooKeeperUtils
import com.datastax.driver.core._
import java.util.concurrent.ConcurrentHashMap
import com.datastax.driver.core.exceptions.{QueryValidationException, QueryExecutionException, NoHostAvailableException}
import scala.Some
import scala.Some
import scala.collection.JavaConverters._
import com.google.common.base.Charsets


/**
 * Handles storage and retrieval of job and task level data within the cluster via Cassandra.
 * @author Andreas C. Osowski (andreas.osowski@newzly.com
 */

class MesosStateCassandraPersistenceStore @Inject()(val config: SchedulerConfiguration with CassandraConfiguration,
                                                    val clusterBuilder: Option[Cluster.Builder])
  extends PersistenceStore {

  val log = Logger.getLogger(getClass.getName)
  var _session: Option[Session] = None
  val statements = new ConcurrentHashMap[String, PreparedStatement]().asScala

  val _tasksTable: String = config.cassandraTasksTable()
  val _jobsTable: String = config.cassandraJobsTable()
  val _cassandraTtl: Int = config.cassandraTtl()

  def getSession: Option[Session] = {
    _session match {
      case Some(s) => _session
      case None =>
        clusterBuilder match {
          case Some(c) =>
            val session = c.build.connect(config.cassandraDataKeyspace())
            session.execute(new SimpleStatement(
              s"CREATE TABLE IF NOT EXISTS ${_tasksTable}" +
                """
                  |(
                  |   name           VARCHAR,
                  |   data           VARCHAR,
                  | PRIMARY KEY (name))
                  | WITH bloom_filter_fp_chance=0.100000 AND
                  | compaction = {'class':'LeveledCompactionStrategy'}
                """.stripMargin
            ))

            session.execute(new SimpleStatement(
              s"CREATE TABLE IF NOT EXISTS ${_jobsTable}" +
                """
                  |(
                  |   name           VARCHAR,
                  |   data           VARCHAR,
                  | PRIMARY KEY (name))
                  | WITH bloom_filter_fp_chance=0.100000 AND
                  | compaction = {'class':'LeveledCompactionStrategy'}
                """.stripMargin
            ))
            _session = Some(session)
            _session
          case None => {
            log.warning("Cassandra cluster builder is None!")
            None
          }
        }
    }
  }

  def resetSession() {
    statements.clear()
    _session match {
      case Some(session) =>
        session.close()
      case _ =>
    }
    _session = None
  }

  def persistJob(job: BaseJob): Boolean = {
    log.info("Persisting job '%s' with data '%s'" format(job.name, job.toString))
    persistData(_jobsTable, job.name, JobUtils.toString(job))
  }

  def persistTask(name: String, data: Array[Byte]): Boolean = {
    log.finest("Persisting task: " + name)
    persistData(_tasksTable, name, new String(data, Charsets.UTF_8))
  }


  // What if this fails? Currently only used inside a test...
  override def getJob(name: String): BaseJob = wrapDbCall((session) => {
    val query = s"SELECT data FROM ${_jobsTable} WHERE name = ?"
    val resultSet = session.execute(query).one()
    JobUtils.fromString(resultSet.getString(0))
  }).get

  override def getJobs: Iterator[BaseJob] = wrapDbCall((session) => {
    val query = s"SELECT * FROM ${_jobsTable}"
    val prepared = preparedStatement(session, query)
    val resultSet = session.execute(prepared.bind())

    resultSet.all().asScala.map((r: Row) => JobUtils.fromString(r.getString(1))).iterator
  }).get

  override def getTasks: Map[String, Array[Byte]] = wrapDbCall((session) => {
    val query = s"SELECT * FROM ${_tasksTable}"
    val prepared = preparedStatement(session, query)
    val resultSet = session.execute(prepared.bind())

    resultSet.all().asScala.map((r: Row) => (r.getString(0), r.getString(1).getBytes(Charsets.UTF_8))).toMap
  }).get

  def removeJob(job: BaseJob): Unit = removeData(_jobsTable, job.name)

  def removeTask(taskId: String): Boolean = removeData(_tasksTable, taskId)

  def purgeTasks(): Unit = wrapDbCall((session) => {
    val query = s"TRUNCATE ${_tasksTable}"
    val prepared = preparedStatement(session, query)
    session.execute(prepared.bind())
  }).get


  override def getTaskIds(filter: Option[String]): List[String] = wrapDbCall((session) => {
    import scala.collection.JavaConversions._

    val query = s"SELECT name FROM ${_tasksTable}"
    val prepared = preparedStatement(session, query)
    val resultSet = session.execute(prepared.bind())

    resultSet.all().map((r: Row) => r.getString(0)).toList
  }).get

  def removeData(table: String, name: String): Boolean = wrapDbCall((session) => {
    val query = s"DELETE FROM ${table} WHERE name = ?"
    val prepared = preparedWriteStatement(session, query)

    session.execute(prepared.bind(
      name
    ))
    true
  }).getOrElse(false)


  private def persistData(table: String, name: String, data: String) = wrapDbCall((session) => {
    val query = s"INSERT INTO ${table} (name, data) VALUES (?, ?)"
    val prepared = preparedWriteStatement(session, query)

    session.execute(prepared.bind(
      name,
      data
    ))
    true
  }).getOrElse(false)

  private def preparedStatement(session: Session, query: String) = statements.getOrElseUpdate(query, {
    session.prepare(
      new SimpleStatement(query).asInstanceOf[RegularStatement]
    )
  })

  private def preparedWriteStatement(session: Session, query: String) = statements.getOrElseUpdate(query, {
    session.prepare(
      new SimpleStatement(query).setConsistencyLevel(ConsistencyLevel.valueOf(config.cassandraConsistency())).asInstanceOf[RegularStatement]
    )
  })

  private def wrapDbCall[T](fn: (Session) => T): Option[T] = {
    try {
      getSession match {
        case Some(session: Session) => return Some(fn(session))
        case None =>
      }
    } catch {
      case e: NoHostAvailableException =>
        resetSession()
        log.log(Level.WARNING, "No hosts were available, will retry next time.", e)
      case e: QueryExecutionException =>
        log.log(Level.WARNING, "Query execution failed:", e)
      case e: QueryValidationException =>
        log.log(Level.WARNING, "Query validation failed:", e)
    }
    None
  }
}
