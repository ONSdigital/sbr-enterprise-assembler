package util

import java.lang
import java.net.ConnectException

import org.apache.curator.framework.recipes.atomic.{AtomicValue, DistributedAtomicLong}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryOneTime

/**
  * Generates a unique sequence number for Hbase/Hive
  *
  * @param hostName             the zookeeper host and port
  * @param resultFormat         the format of the results. The default is 11%07d, which corresponds to the SBR requirement
  * @param path                 the Zookeeper path to store the counter value
  * @param sessionTimeoutSec    session timeout in seconds
  * @param connectionTimeoutSec connection timeout in seconds
  */
class SequenceGenerator(
                         hostName: String,
                         resultFormat: String = "11%07d",
                         path: String = "/ids/enterprise/id",
                         sessionTimeoutSec: Int = 5,
                         connectionTimeoutSec: Int = 5
                       ) extends Serializable {

  private val client: CuratorFramework = CuratorFrameworkFactory.newClient(hostName, sessionTimeoutSec * 1000,
    connectionTimeoutSec * 1000, new RetryOneTime(1))
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

