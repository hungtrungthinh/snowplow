/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich
package spark

// Apache commons
import org.apache.commons.codec.binary.Base64

// Scalaz
import scalaz._
import Scalaz._

// Spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.col

// Snowplow
import common.{EtlPipeline, FatalEtlError, ValidatedEnrichedEvent}
import common.loaders.{Loader, ThriftLoader}
import common.outputs.{BadRow, EnrichedEvent}

object EnrichJob extends SparkJob {
  private[spark] val classesToRegister: Array[Class[_]] = Array(
    classOf[Array[String]],
    classOf[Array[Array[Byte]]],
    classOf[EnrichedEvent],
    classOf[scala.collection.immutable.Map$EmptyMap$],
    classOf[scala.collection.immutable.Set$EmptySet$],
    classOf[scala.collection.mutable.WrappedArray$ofRef],
    classOf[org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage]
  )
  override def sparkConfig(): SparkConf = new SparkConf()
    .setAppName(getClass().getSimpleName())
    .setIfMissing("spark.master", "local[*]")
    .set("spark.serializer", classOf[KryoSerializer].getName())
    .set("spark.kryo.registrationRequired", "true")
    .registerKryoClasses(classesToRegister)

  override def run(spark: SparkSession, args: Array[String]): Unit = {
    val job = EnrichJob(spark, args)
    job.run()
  }

  def apply(spark: SparkSession, args: Array[String]) = new EnrichJob(spark, args)

  val etlVersion = s"spark-${generated.ProjectSettings.version}"

  /**
   * Project our Failures into a List of Nel of strings.
   * @param all A List of Validations each containing either an EnrichedEvent or Failure strings
   * @return A (possibly empty) List of failures, where each failure is a Nel of strings
   */
  def projectBads(all: List[ValidatedEnrichedEvent]): List[NonEmptyList[String]] =
    all.collect { case Failure(errs) => errs }

  /**
   * Project our Sucesses into EnrichedEvents.
   * @param all A List of Validations each containing either an EnrichedEvent or Failure strings
   * @return A (possibly empty) List of EnrichedEvent
   */
  def projectGoods(all: List[ValidatedEnrichedEvent]): List[EnrichedEvent] =
    all.collect { case Success(e) => e }
}

/**
 * The Snowplow Enrich job, written in Spark.
 * @param spark Spark session used throughout the job
 * @param args Command line arguments for the enrich job
 */
class EnrichJob(@transient val spark: SparkSession, args: Array[String]) extends Serializable {
  @transient private val sc: SparkContext = spark.sparkContext
  import spark.implicits._

  // Necessary to be able to deal with LZO-compressed files
  @transient private val hadoopConfig = sc.hadoopConfiguration
  hadoopConfig.set("io.compression.codecs", classOf[com.hadoop.compression.lzo.LzopCodec].getName())
  hadoopConfig.set("io.compression.codec.lzo.class",
    classOf[com.hadoop.compression.lzo.LzoCodec].getName())

  // Job configuration
  private val enrichConfig = EnrichJobConfig.loadConfigFrom(args).fold(
    e => throw FatalEtlError(e.map(_.toString)),
    identity
  )

  def run(): Unit = {
    import EnrichJob._
    import singleton._

    // Install MaxMind file(s) if we have them
    enrichConfig.filesToCache.foreach { case (uri, filename) => sc.addFile(uri.toString) }

    val inFormat = enrichConfig.inFormat
    val igluConfig = enrichConfig.igluConfig

    val input = getInputRDD(inFormat, enrichConfig.inFolder)

    val common = input
      .map { line =>
        (
          line,
          EtlPipeline.processEvents(
            RegistrySingleton.get(igluConfig, enrichConfig.enrichments, enrichConfig.local),
            etlVersion,
            enrichConfig.etlTstamp,
            LoaderSingleton.get(inFormat).asInstanceOf[Loader[Any]].toCollectorPayload(line)
          )(ResolverSingleton.get(igluConfig))
        )
      }
      .cache()

    // Handling of malformed rows
    val bad = common
      .map { case (line, enriched) => (line, projectBads(enriched)) }
      .flatMap { case (line, errors) =>
        val originalLine = line match {
          case bytes: Array[Byte] => new String(Base64.encodeBase64(bytes), "UTF-8")
          case other => other.toString
        }
        errors.map(BadRow(originalLine, _).toCompactJson)
      }
    if (!bad.isEmpty) bad.saveAsTextFile(enrichConfig.badFolder)

    // Handling of properly-formed rows
    val good = common
      .flatMap { case (line, enriched) => projectGoods(enriched) }
    if (!good.isEmpty)
      spark.createDataset(good)(Encoders.bean(classOf[EnrichedEvent]))
        .toDF()
        // hack to preserve the order of the fields in the csv, otherwise it's alphabetical
        .select(classOf[EnrichedEvent].getDeclaredFields().map(f => col(f.getName())): _*)
        .write
        .option("sep", "\t")
        .option("escape", "")
        .option("quote", "")
        .csv(enrichConfig.outFolder)
  }

  /**
   * Read the input files.
   * @param inFormat Collector format in which the data is coming in
   * @return A RDD containing strings or byte arrays
   */
  private def getInputRDD(inFormat: String, path: String): RDD[_] = {
    inFormat match {
      case "thrift" =>
        import com.twitter.elephantbird.mapreduce.input.MultiInputFormat
        MultiInputFormat.setClassConf(classOf[Array[Byte]], hadoopConfig)
        sc.newAPIHadoopFile[
          org.apache.hadoop.io.LongWritable,
          com.twitter.elephantbird.mapreduce.io.BinaryWritable[Array[Byte]],
          MultiInputFormat[Array[Byte]]
        ](path)
          .map(_._2.get())
      case _ => sc.textFile(path)
    }
  }
}
