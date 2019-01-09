package util

import java.lang
import java.net.ConnectException

import org.apache.curator.RetryPolicy
import org.apache.curator.framework.recipes.atomic.{AtomicValue, DistributedAtomicLong}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.{ExponentialBackoffRetry, RetryOneTime}
import util.configuration.AssemblerConfiguration._
/**
  * Generates a unique sequence number for HBase/Hive
  */
object SequenceGenerator {

  val hostName: String = SequenceURL
  val resultFormat: String = SequenceFormat
  val path: String = SequencePath
  val sessionTimeoutSec: Int = SequenceSessionTimeout.toInt
  val connectionTimeoutSec: Int = SequenceConnectionTimeout.toInt

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

