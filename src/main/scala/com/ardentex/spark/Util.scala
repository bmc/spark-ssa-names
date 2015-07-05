package com.ardentex.spark

import java.io.File

import org.apache.spark.rdd.RDD

import scala.util.Try

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


/** Utility functions used by the various programs in this package.
  *
  */
object Util {

  private val ReNewline = "[\n\r]+".r
  private val ReCSV     = """\s*,\s*""".r

  /** Parsed arguments. Assumption: All programs accept the same command line
    * arguments (currently the case).
    *
    * @param master      The Spark master
    * @param dataDir     The directory containing the data files
    * @param cores       The number of Spark cores (tasks) to use
    * @param parquetPath The path to the output Parquet table
    */
  case class Args(master: String, dataDir: File, cores: Int, parquetPath: String)

  /** Split a string containing multiple lines into an array of lines, handling
    * any cross-platform newline differences. Blank lines are removed.
    *
    * @param data  the data, consisting of multiple lines
    *
    * @return an array of the lines, with an blank lines removed.
    */
  def splitLines(data: String): Array[String] = {
    ReNewline.split(data).map { _.trim }.filter { _.length > 0 }
  }

  /** Split a CSV line. This function is simple and assumes that the data
    * is not a true CSV file with quoting, but just a simple comma-separated
    * list of tokens.
    *
    * @param line  the line
    *
    * @return An array of the cells
    */
  def csvSplit(line: String): Array[String] = ReCSV.split(line)

  /** Parse the command line arguments. Assumes all programs use the same
    * command line format (which is currently the case).
    *
    * @param className   the className of the object containing main()
    * @param args        the command line arguments
    *
    * @return `Success(Args)` or `Failure(error)`
    */
  def parseArgs(className: String, args: Array[String]): Try[Args] = {
    Try {
      // Strip the "$" from the end of the object name. That's what the class
      // name looks like to the JVM at runtime, but not how it's invoked.
      val name = convertClassName(className)

      val parsedArgs = args.length match {
        case 3 => Args(args(0), new File(args(1)), 1, args(2))
        case 4 => Args(args(0), new File(args(1)), args(3).toInt, args(2))
        case _ => {
          throw new Exception(s"Usage: $name spark_master data_dir " +
                              "parquet_file [cores]")
        }
      }

      if (! parsedArgs.dataDir.exists) {
        throw new Exception(s"Data directory ${parsedArgs.dataDir} doesn't exist.")
      }

      if (! parsedArgs.dataDir.isDirectory) {
        throw new Exception(s"${parsedArgs.dataDir} isn't a directory.")
      }

      parsedArgs
    }
  }

  /** Convenience function: Sets up the Spark and Spark SQL contexts and
    * invokes the `process` function to run the actual Spark job.
    *
    * @param className   the class name of the invoking program
    * @param sparkMaster Spark master to use
    * @param process     the function to process the job
    */
  def runSpark(className:   String,
               sparkMaster: String)
              (process:     (SparkContext, SQLContext) => Unit): Unit = {

     val conf = (new SparkConf).setAppName(convertClassName(className)).
                                setMaster(sparkMaster)
     val sc = new SparkContext(conf)
     process(sc, new SQLContext(sc))
  }

  private def convertClassName(className: String) = {
    // Strip the "$" from the end of the object name. That's what the class
    // name looks like to the JVM at runtime, but not how it's invoked.
    className.replace("$", "")
  }
}
