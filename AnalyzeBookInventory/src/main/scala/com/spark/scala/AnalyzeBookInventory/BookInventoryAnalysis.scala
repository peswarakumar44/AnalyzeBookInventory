package com.spark.scala.AnalyzeBookInventory

import java.sql.Connection
import java.sql.DriverManager
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.commons.lang3.SystemUtils

object BookInventoryAnalysis {

  def main(args: Array[String]): Unit = {

    val BookCustomSchema = StructType(
      Array(
        StructField("_year", LongType, nullable = true),
        StructField("author", StringType, nullable = true),
        StructField("isbn", LongType, nullable = true),
        StructField("price", DoubleType, nullable = true),
        StructField("publisher", StringType, nullable = true),
        StructField("title", StringType, nullable = true),
        StructField("ia_isbns", LongType, nullable = true),
        StructField("isbns", LongType, nullable = true)))

    val hadoopWinUtilDir: String = args(0).trim
    val hadoopLinuxUtilDir: String = args(1).trim
    val xmlReadFormat: String = args(2).trim
    val rootTagVal: String = args(3).trim
    val rowTagVal: String = args(4).trim
    val xmlDataFile: String = args(5).trim
    val txtReadFormat: String = args(6).trim
    val txtDelimit: String = args(7).trim
    val txtIsHdr: String = args(8).trim
    val textDataFile: String = args(9).trim
    val url: String = args(10).trim
    val username: String = args(11).trim
    val password: String = args(12).trim
    val driver: String = args(13).trim
    val conn: Connection = DriverManager.getConnection(url, username, password)
    val struct: StructType = BookCustomSchema
    val table = args(14).trim
    val createTableOptions: String = args(15).trim
    val DDLFlag: String = args(16).trim

    /*
     * Description of Command Line Argument Values:
     * hadoopWinUtilDir = c:\\winutil\\ ===> Download Windows Utils & then create winutil\bin under c:\ then place all of the downloaded Win Utils under c:\winutil\bin
     * hadoopLinuxUtilDir = "" ===> Null String is passed for Linux Util Directory since i am running from Scala IDE Installed on Windows
     * rootTagVal = xmlReadFormat = com.databricks.spark.xml ==> Spark Databricks Read Format of xml
     * rootTagVal=inventory ==> Root Tag of the XML File
     * rowTagVal = book ==> Row Tag of the XML File
     * xmlDataFile = xmldatafiles/inventory.xml ==> XML Eclipse Dir Location & XML Data File Name
     * txtReadFormat = csv ==> Spark Text File Read Format
     * txtDelimit = , ==> Text File Delimitor & Delimitor can be , | or whatever is the delimitor of that file
     * txtIsHdr = true ==> Tells whether Text File has Header or not. it can be true or false based on file's Header availability
     * textDataFile = txtdatafiles/ia_isbns_uniq_master.txt ==> Text File Eclipse Dir Location & Text Data File Name
     * url = jdbc:oracle:thin:@localhost:1521/orcl ==> url for Oracle JDBC Connectivity
     * username = c##scott ==> Oracle User Name or Schema Name
     * password = tiger  ==> Oracle User Password
     * driver = oracle.jdbc.OracleDriver==>  Type of JDBC Driver.. Here it Oracle JDBC Drive
     * table = BOOK_INVENTORY ==> Name of the Table that is expected to be created in Oracle using the Final DataFrame. This Value can be Null String = "" if not Table Object is expected to be created in Oracle
     * createTableOptions = "" ==> As it is basic Table creation, Null String = "" is passed as createTableOptions here
     * DDLFlag = CreateAppend ==> Type of Operation is required at the DB side.. The Operations can be either the items of list: CreateAppend|OnlyReAppend|TruncateReAppend|DropReCreateReAppend
     *
     * Command Line Argument Example1 (CreateAppend=Creates New Table & Loads Data): 
     * c:\\winutil\\ "" com.databricks.spark.xml inventory book xmldatafiles/inventory.xml csv , true txtdatafiles/ia_isbns_uniq_master.txt jdbc:oracle:thin:@localhost:1521/orcl c##scott tiger oracle.jdbc.OracleDriver BOOK_INVENTORY "" CreateAppend
     *
     * Command Line Argument Example2 (OnlyReAppend: Incremental Addition of Data to Existing) : 
     * c:\\winutil\\ "" com.databricks.spark.xml inventory book xmldatafiles/inventory.xml csv , true txtdatafiles/ia_isbns_uniq_master.txt jdbc:oracle:thin:@localhost:1521/orcl c##scott tiger oracle.jdbc.OracleDriver BOOK_INVENTORY "" OnlyReAppend
     *
     * Command Line Argument Example3 (TruncateReAppend: If Errored Data is Loaded Truncate total Data & Reload a Fresh Data to Table) : 
     * c:\\winutil\\ "" com.databricks.spark.xml inventory book xmldatafiles/inventory.xml csv , true txtdatafiles/ia_isbns_uniq_master.txt jdbc:oracle:thin:@localhost:1521/orcl c##scott tiger oracle.jdbc.OracleDriver BOOK_INVENTORY "" TruncateReAppend
     *
     * Command Line Argument Example4 (DropReCreateReAppend: If Structure of Table is Altered due to the Enhancements in the DataFrames, DropReCreateReAppend is useful) : 
     * c:\\winutil\\ "" com.databricks.spark.xml inventory book xmldatafiles/inventory.xml csv , true txtdatafiles/ia_isbns_uniq_master.txt jdbc:oracle:thin:@localhost:1521/orcl c##scott tiger oracle.jdbc.OracleDriver BOOK_INVENTORY "" DropReCreateReAppend
     */

    val isWindows: Boolean = SystemUtils.IS_OS_WINDOWS
    val isLinux: Boolean = SystemUtils.IS_OS_LINUX

    if (isWindows == true) {

      println("Setting the Hodoop Home Dir of Windows for WinUtilJars because SystemUtils.IS_OS_WINDOWS is :" + isWindows)
      System.setProperty("hadoop.home.dir", hadoopWinUtilDir)

    } else if (isLinux == true) {

      println("Setting the Hodoop Home Dir of Linux because SystemUtils.IS_OS_LINUX is :" + isLinux)
      System.setProperty("hadoop.home.dir", hadoopLinuxUtilDir)

    } else {

      println("Set Appropriate Hodoop Home based on OS Type")

    }

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("XML Processing").master("local").getOrCreate()

    val df = readBookInventory(spark, xmlReadFormat, rootTagVal, rowTagVal, xmlDataFile, txtReadFormat, txtDelimit, txtIsHdr, textDataFile)

    val ref = new OracleDBSaver(url, username, password, driver, conn, struct, table, createTableOptions)

    ref.createAppendTruncDropReCreateOraDbTable(df, DDLFlag)

  }

  def readBookInventory(spark: SparkSession, xmlReadFormatInfo: String, rootTagValue: String, rowTagValue: String, xmlDataFileInfo: String, txtReadFormatInfo: String, txtDelimitor: String, txtIsHeader: String, textDataFileInfo: String) = {

    import spark.implicits._

    //val bookdf = spark.read.option("rowTag", "book").xml("xmldatafiles/inventory.xml")

    val bookdf = spark.read.format(xmlReadFormatInfo).option("rootTag", rootTagValue).option("rowTag", rowTagValue).load(xmlDataFileInfo)
    val flatbookdf = bookdf.withColumn("author", explode($"author")) // Flattening
    val isbndf = spark.read.format(txtReadFormatInfo).option("delimiter", txtDelimitor).option("header", txtIsHeader).load(textDataFileInfo).withColumn("isbns", expr("substring(ia_isbns, 5, length(ia_isbns)-1)")).selectExpr("cast(ia_isbns as long) ia_isbns", "cast(isbns as long) isbns")
    val bookinventorydf = flatbookdf.join(isbndf, $"isbn" === $"isbns", "inner")

    bookdf.show()
    bookdf.printSchema() // Nested Schema

    flatbookdf.show()
    flatbookdf.printSchema() // Flat Schema

    isbndf.show()
    isbndf.printSchema()

    bookinventorydf.show()
    bookinventorydf.printSchema()

    bookinventorydf

  }

}