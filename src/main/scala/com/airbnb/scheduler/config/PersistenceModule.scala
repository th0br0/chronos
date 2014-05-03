package com.airbnb.scheduler.config

import com.google.inject.{Singleton, Inject, AbstractModule, Provides}
import com.datastax.driver.core.Cluster
import com.airbnb.scheduler.state.{MesosStateZooKeeperPersistenceStore, PersistenceStore, MesosStateCassandraPersistenceStore}
import com.twitter.common.zookeeper.{ZooKeeperUtils, ZooKeeperClient}
import org.apache.mesos.state.State
import org.apache.zookeeper.ZooDefs

/**
 * @author Andreas C. Osowski  (andreas.osowski@newzly.com)
 */
class PersistenceModule(val config: SchedulerConfiguration with CassandraConfiguration)
  extends AbstractModule {

  def configure() {}

  @Inject
  @Singleton
  @Provides
  def provideStore(clusterBuilder: Option[Cluster.Builder], zk: ZooKeeperClient, state: State): PersistenceStore = {
    config.persistTo() match {
      case "cassandra" =>
        new MesosStateCassandraPersistenceStore(config, clusterBuilder)
      case _ => {
        ZooKeeperUtils.ensurePath(zk,
          ZooDefs.Ids.OPEN_ACL_UNSAFE,
          config.zooKeeperStatePath)

        new MesosStateZooKeeperPersistenceStore(zk, config, state)
      }
    }
  }
}
