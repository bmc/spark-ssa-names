package com.ardentex.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/** This program is a simple Spark ETL program that takes a directory full
  * of Social Security Administration baby names data files and loads them
  * into an Apache Parquet file, making the data more easily consumed by
  * other Spark jobs.
  *
  * The data can be found at
  * http://catalog.data.gov/dataset/baby-names-from-social-security-card-applications-national-level-data
  */
object SSNNamesToParquet {

  /** Parsed name data.
    *
    * @param firstName  the first name (i.e., the baby name)
    * @param gender     the gender ("F" or "M")
    * @param total      the total
    * @param year       the year (inferred from the file name)
    */
  case class NameData(firstName: String, gender: String, total: Int, year: Int)

  // Used to extract the year from the file name and also to test whether a
  // particular file is a data file.
  private val YearExtractor   = """^.*/yob(\d+)\.txt$""".r

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

        // Map over each file, producing a new RDD containing NameData objects.

        val entriesRDD = dirRDD.flatMap {
          case (fileURL, contents) => {
            fileURL match {
              case YearExtractor(year) => processYear(year.toInt, contents)

              case _ => {
                println(s"Skipping ${fileURL}: It isn't a data file.")
                Array.empty[NameData]
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

  /** Convert the data file for a particular year into an array of
    * `NameData` objects.
    *
    * @param year     The year
    * @param contents The contents of the file, as a string
    *
    * @return The array of `NameData` objects
    */
  private def processYear(year: Int, contents: String): Array[NameData] = {

    println(s"*** Processing ${year}.")

    for { row   <- Util.splitLines(contents)
          cells =  Util.csvSplit(row) }
      yield NameData(firstName = cells(0),
                     gender    = cells(1),
                     total     = cells(2).toInt,
                     year      = year)
  }

  /** Write the contents of an RDD of NameData objects to the output
    * Parquet table.
    *
    * @param rdd        The RDD
    * @param sqlContext The current sqlContext
    * @param output     The path to the Parquet table
    */
  private def writeParquet(rdd:        RDD[NameData],
                           sqlContext: SQLContext,
                           output:     String): Unit = {
    import sqlContext.implicits._

    val df = rdd.toDF
    df.write.parquet(output)
    println(s"*** Wrote ${output}")
  }
}
