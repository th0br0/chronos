package com.airbnb.scheduler.config

import org.rogach.scallop.ScallopConf

trait CassandraConfiguration extends ScallopConf {

  lazy val cassandraContactPoints = opt[String]("cassandra_contact_points",
    descr = "Comma separated list of contact points for Cassandra",
    default = None)

  lazy val cassandraPort = opt[Int]("cassandra_port",
    descr = "Port for Cassandra",
    default = Some(9042))

  lazy val cassandraStatsKeyspace = opt[String]("cassandra_stats_keyspace",
    descr = "Keyspace to use for stats persisted to Cassandra",
    default = Some("metrics"))

  lazy val cassandraDataKeyspace = opt[String]("cassandra_data_keyspace",
    descr = "Keyspace to use for data persisted to Cassandra",
    default = Some("chronos"))

  lazy val cassandraStatsTable = opt[String]("cassandra_stats_table",
    descr = "Table to use for stats persisted to Cassandra",
    default = Some("stats"))

  lazy val cassandraTasksTable = opt[String]("cassandra_tasks_table",
    descr = "Table to use for tasks persisted to Cassandra",
    default = Some("tasks"))
  lazy val cassandraJobsTable = opt[String]("cassandra_jobs_table",
    descr = "Table to use for jobs persisted to Cassandra",
    default = Some("jobs"))

  lazy val cassandraConsistency = opt[String]("cassandra_consistency",
    descr = "Consistency to use for Cassandra writes",
    default = Some("ANY"))

  lazy val cassandraTtl = opt[Int]("cassandra_ttl",
    descr = "TTL for records written to Cassandra",
    default = Some(3600*24*365))
}
