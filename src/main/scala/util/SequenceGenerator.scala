package util

import java.lang
import java.net.ConnectException

import org.apache.curator.RetryPolicy
import org.apache.curator.framework.recipes.atomic.{AtomicValue, DistributedAtomicLong}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.{ExponentialBackoffRetry, RetryOneTime}
import util.options.ConfigOptions

/**
  * Generates a unique sequence number for Hbase/Hive
  *
  * @param hostName             the zookeeper host and port
  * @param resultFormat         the format of the results. The default is 11%07d, which corresponds to the SBR requirement
  * @param path                 the Zookeeper path to store the counter value
  * @param sessionTimeoutSec    session timeout in seconds
  * @param connectionTimeoutSec connection timeout in seconds
  */
object SequenceGenerator {

  //val hostName: String = ConfigOptions.SequenceURL

  val hostName: String = ConfigOptions.HBaseZookeeperQuorum.split(",").toList.map( x => x + ":" + ConfigOptions.HBaseZookeeperPort).mkString(",")
  println("HBaseZookeeperQuorum" + ConfigOptions.HBaseZookeeperQuorum)
  println("HBaseLinksTableNamespace" + ConfigOptions.HBaseLinksTableNamespace)
  val resultFormat: String = ConfigOptions.SequenceFormat
  val path: String = ConfigOptions.SequencePath
  val sessionTimeoutSec: Int = ConfigOptions.SequenceSessionTimeout.toInt
  val connectionTimeoutSec: Int = ConfigOptions.SequenceConnectionTimeout.toInt

  private val retryPolicy: RetryPolicy = new ExponentialBackoffRetry(1000, 3)
  private val client: CuratorFramework = CuratorFrameworkFactory.newClient(hostName, sessionTimeoutSec * 1000,
    connectionTimeoutSec.toInt * 1000, retryPolicy)
  private val dal: DistributedAtomicLong = new DistributedAtomicLong(client, path, new RetryOneTime(1))

  client.start()
  client.getZookeeperClient.blockUntilConnectedOrTimedOut()

  if (!client.getZookeeperClient.blockUntilConnectedOrTimedOut())
    throw new ConnectException(s"Connection to Zookeeper timed out, host: $hostName")

  def nextSequence: String = nextSequence(1)._1

  def nextSequence(batchSize: Long): (String, String) = {
    val id: AtomicValue[lang.Long] = dal.add(batchSize)

    resultFormat.format(id.preValue + 1) -> resultFormat.format(id.postValue.longValue)
  }

  def close(): Unit = client.close()

  def currentSequence: String = resultFormat.format(dal.get().postValue().longValue)
}

