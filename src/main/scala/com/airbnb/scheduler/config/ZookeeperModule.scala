package com.airbnb.scheduler.config

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import java.util.logging.Logger

import com.airbnb.scheduler.state.{PersistenceStore, MesosStateZooKeeperPersistenceStore}
import com.google.inject._
import com.twitter.common.quantity.{Time, Amount}
import com.twitter.common.zookeeper._
import org.apache.mesos.state.{State, ZooKeeperState}
import org.apache.zookeeper.ZooDefs
import mesosphere.mesos.util.FrameworkIdUtil
import mesosphere.chaos.http.HttpConf
import com.twitter.common.base.Supplier

/**
 * Guice glue-code for zookeeper related things.
 * @author Florian Leibert (flo@leibert.de)
 */
//TODO(FL): Consider using Sindi or Subcut for DI.
class ZookeeperModule(val config: SchedulerConfiguration with HttpConf)
  extends AbstractModule {
  private val log = Logger.getLogger(getClass.getName)

  def configure() {}

  @Inject
  @Singleton
  @Provides
  def provideZookeeperClient(): ZooKeeperClient = {
    import collection.JavaConversions._
    new ZooKeeperClient(
      Amount.of(config.zooKeeperTimeout().toInt, Time.MILLISECONDS),
      parseZkServers())
  }

  @Inject
  @Singleton
  @Provides
  def provideState(): State = {
    new ZooKeeperState(config.zookeeperServers(),
      config.zooKeeperTimeout(),
      TimeUnit.MILLISECONDS,
      config.zooKeeperStatePath)
  }

  @Provides
  @Singleton
  def provideFrameworkIdUtil(state: State): FrameworkIdUtil = {
    new FrameworkIdUtil(state)
  }

  @Inject
  @Singleton
  @Provides
  def provideCandidate(zk: ZooKeeperClient): Candidate = {
    log.info("Using hostname:" + config.hostname())
    return new CandidateImpl(new Group(zk, ZooDefs.Ids.OPEN_ACL_UNSAFE,
      config.zooKeeperCandidatePath),
      new Supplier[Array[Byte]] {
        def get() = {
          "%s:%d".format(config.hostname(), config.httpPort()).getBytes
        }
      })
  }

  private def parseZkServers(): List[InetSocketAddress] = {
    val servers = config.zookeeperServers().split(",")
    servers.map({
      server =>
        require(server.split(":").size == 2, "Error, zookeeper servers must be provided in the form host1:port2,host2:port2")
        new InetSocketAddress(server.split(":")(0), server.split(":")(1).toInt)
    }).toList
  }
}
