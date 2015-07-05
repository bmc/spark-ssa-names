package com.ardentex.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/** This program is a simple Spark ETL program that takes a directory full
  * of Social Security Administration baby names data files, broken out by
  * state, and loads them into an Apache Parquet file, making the data more
  * easily consumed by other Spark jobs.
  *
  * The data can be found at
  * https://catalog.data.gov/dataset/baby-names-from-social-security-card-applications-data-by-state-and-district-of-
  */
object SSNNamesByStateToParquet {

  /** Parsed name data.
    *
    * @param firstName  the first name (i.e., the baby name)
    * @param gender     the gender ("F" or "M")
    * @param total      the total
    * @param year       the year (inferred from the file name)
    * @param state      the state
    */
  case class StateNameData(firstName: String,
                           gender:    String,
                           total:     Int,
                           year:      Int,
                           state:     String)

  // Used to validate the data file name.
  private val DataFilePattern = """^.*/([A-Z]{2}).TXT$""".r

  /** Main program
    *
    * @param args command line arguments
    */
  def main(args: Array[String]): Unit = {
    val className = this.getClass.getName
    Util.parseArgs(className, args) map { parsedArgs =>
      Util.runSpark(className, parsedArgs.master) { (sc, sqlContext) =>

        // Create an RDD using wholeTextFiles(), which specifies a directory.
        // The resulting RDD will be an Array[(String, String)], where the
        // first string is the URL for a file in the directory and the second
        // string is that file's complete contents.

        val dirRDD = sc.wholeTextFiles(parsedArgs.dataDir.getPath)

        // Map over each file, producing a new RDD of StateNameData objects.

        val entriesRDD = dirRDD.flatMap {
          case (fileURL, contents) => {
            fileURL match {
              case DataFilePattern(state) => processState(state, contents)

              case _ => {
                println(s"Skipping ${fileURL}: It isn't a data file")
                Array.empty[StateNameData]
              }
            }
          }
        }

        writeParquet(entriesRDD, sqlContext, parsedArgs.parquetPath)
      }
    } recover {
      case e: Exception => {
        System.err.println(e.getMessage)
        System.exit(1)
      }
    }
  }

  /** Convert the data file for a particular state into an array of
    * `StateNameData` objects.
    *
    * @param state
    * @param contents
    * @return
    */
  private def processState(state:    String,
                           contents: String): Array[StateNameData] = {
    import Util._

    println(s"*** Processing state ${state}.")

    for { row  <- splitLines(contents)
          cells = csvSplit(row) if (cells.length == 5) && (cells(0) == state) }
      yield StateNameData(state     = cells(0),
                          gender    = cells(1),
                          year      = cells(2).toInt,
                          firstName = cells(3),
                          total     = cells(4).toInt)
  }

  /** Write the contents of an RDD of StateNameData objects to the output
    * Parquet table.
    *
    * @param rdd        The RDD
    * @param sqlContext The current sqlContext
    * @param output     The path to the Parquet table
    */
  private def writeParquet(rdd:        RDD[StateNameData],
                           sqlContext: SQLContext,
                           output:     String): Unit = {
    import sqlContext.implicits._

    val df = rdd.toDF
    df.write.parquet(output)
    println(s"*** Wrote ${output}")
  }

}
