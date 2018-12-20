package closures

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Partitioner

private object HFilePartitioner {
  def apply(conf: Configuration, splits: Array[Array[Byte]], numFilesPerRegionPerFamily: Int): HFilePartitioner = {
    if (numFilesPerRegionPerFamily == 1)
      new SingleHFilePartitioner(splits)
    else {
      val fraction = 1 max numFilesPerRegionPerFamily min conf.getInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 32)
      new MultiHFilePartitioner(splits, fraction)
    }
  }
}

protected abstract class HFilePartitioner extends Partitioner {
  def extractKey(n: Any): Array[Byte] = n match {
    case k: String => Bytes.toBytes(k)
    case (k: String, _) => Bytes.toBytes(k)
  }
}

private class MultiHFilePartitioner(splits: Array[Array[Byte]], fraction: Int) extends HFilePartitioner {
  override def getPartition(key: Any): Int = {
    val k = extractKey(key)
    val h = (k.hashCode() & Int.MaxValue) % fraction
    for (i <- 1 until splits.length)
      if (Bytes.compareTo(k, splits(i)) < 0) return (i - 1) * fraction + h

    (splits.length - 1) * fraction + h
  }

  override def numPartitions: Int = splits.length * fraction
}

private class SingleHFilePartitioner(splits: Array[Array[Byte]]) extends HFilePartitioner {
  override def getPartition(key: Any): Int = {
    val k = extractKey(key)
    for (i <- 1 until splits.length)
      if (Bytes.compareTo(k, splits(i)) < 0) return i - 1

    splits.length - 1
  }

  override def numPartitions: Int = splits.length
}
