package com.spark.scala.AnalyzeBookInventory

import java.sql.Connection
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType

class OracleDBSaver(_url: String, _username: String, _password: String, _driver: String, _conn: Connection, _struct: StructType, _table: String, _createTableOptions: String) {

  val OracleDialect = new JdbcDialect {
    override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle") || url.contains("oracle")

    override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
      case StringType    => Some(JdbcType("VARCHAR2(255)", java.sql.Types.VARCHAR))
      case BooleanType   => Some(JdbcType("NUMBER(1)", java.sql.Types.NUMERIC))
      case IntegerType   => Some(JdbcType("NUMBER(10)", java.sql.Types.NUMERIC))
      case LongType      => Some(JdbcType("NUMBER(19)", java.sql.Types.NUMERIC))
      case DoubleType    => Some(JdbcType("NUMBER(19,4)", java.sql.Types.NUMERIC))
      case FloatType     => Some(JdbcType("NUMBER(19,4)", java.sql.Types.NUMERIC))
      case ShortType     => Some(JdbcType("NUMBER(5)", java.sql.Types.NUMERIC))
      case ByteType      => Some(JdbcType("NUMBER(3)", java.sql.Types.NUMERIC))
      case BinaryType    => Some(JdbcType("BLOB", java.sql.Types.BLOB))
      case TimestampType => Some(JdbcType("DATE", java.sql.Types.DATE))
      case DateType      => Some(JdbcType("DATE", java.sql.Types.DATE))
      case _             => None
    }

  }

  JdbcDialects.registerDialect(OracleDialect)

  val url = _url
  val username = _username
  val password = _password
  val driver = _driver
  val conn = _conn
  val table = _table
  val struct = _struct
  val createTableOptions = _createTableOptions

  private def getProps: JDBCOptions = {

    println("Preparing the JDBCOption Properties")

    val props = new JDBCOptions(Map("url" -> url, "dbtable" -> table, "user" -> username, "password" -> password, "driver" -> driver))

    props
  }

  def createAppendTruncDropReCreateOraDbTable(df: DataFrame, DDLFlag: String): Unit = {

    println(s"Evaluating the Table: $table is available in OracleDb")

    val isTableExistFlag: Boolean = org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.tableExists(conn, url, table)

    println(s"$table Table Availability Evaluation Result: $isTableExistFlag")

    if (table != "") {

      if (isTableExistFlag == false && DDLFlag == "CreateAppend") {

        println(s"$table Table is not Available in OracleDB")

        println(s"Creating Oracle Table: $table")

        org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.createTable(struct, url, table, createTableOptions, conn)

        println(s"Appending the Data to the newly created Oracle Table: $table")

        org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.saveTable(df, url, table, getProps)

      } else if (isTableExistFlag == false && DDLFlag == "OnlyReAppend") {

        println(s"Data cant be Reappended to the $table Table when $table Table is not in OracleDB")

        println(s"Provided Command Line Argument Value: $DDLFlag is not valid when the $table Table is not in DB")

        println(s"Either provide the table name that is already existing in the Oracle DB to perform OnlyReAppend or provide appropriate value to the $DDLFlag from Command Line Argument")
      } else if (isTableExistFlag == false && DDLFlag == "TruncateReAppend") {

        println(s"Table cant be Truncate & Reappended to the $table Table when $table Table is not in OracleDB")

        println(s"Provided Command Line Argument Value: $DDLFlag is not valid when the $table Table is not in DB")

        println(s"Either provide the table name that is already existing in the Oracle DB to perform TruncateReAppend or provide appropriate value to the $DDLFlag from Command Line Argument")
      } else if (isTableExistFlag == false && DDLFlag == "DropReCreateReAppend") {

        println(s"Table cant be Dropped, Recreated & Reappend to the $table Table when $table Table is not in OracleDB")

        println(s"Provided Command Line Argument Value: $DDLFlag is not valid when the $table Table is not in DB")

        println(s"Either provide the table name that is already existing in the Oracle DB to perform DropReCreateReAppend or provide appropriate value to the $DDLFlag from Command Line Argument")
      } else if (isTableExistFlag == true && DDLFlag == "CreateAppend") {

        println(s"$table Table cant be Created & appended  when a table with same name is already present in oracle DB")

        println(s"Provided Command Line Argument Value: $DDLFlag is not valid because two tables with the same name cant exist in same user of a database")

        println(s"Either provide the Command Line Argument, DropReCreateReAppend as DDLFlag to Drop the existing Table & Recreate with data or DDLFlag as OnlyReAppend to insert the data incrementally ")
      } else {

        if (isTableExistFlag == true && DDLFlag == "OnlyReAppend") {

          println(s"$table Table is Available in OracleDB")

          println(s"Appending the Data to the existing Oracle Table: $table")

          org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.saveTable(df, url, table, getProps)

        } else if (isTableExistFlag == true && DDLFlag == "TruncateReAppend") {
          println(s"Truncating the Data of the Existing Oracle Table: $table")

          org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.truncateTable(conn, table)

          println(s"Re-Appending the Data to the Existing Oracle Table: $table")

          org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.saveTable(df, url, table, getProps)

        } else if (isTableExistFlag == true && DDLFlag == "DropReCreateReAppend") {

          println(s"$table Table is Available in OracleDB but Drop, Recreate & ReAppend Instructons are Issued")

          println(s"Attempting to Drop the Existing Oracle Table: $table")

          org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.dropTable(conn, table)

          println(s"Dropping of the Existing Oracle Table: $table is Successful")

          println(s"Re-Creating Oracle Table: $table")

          org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.createTable(struct, url, table, createTableOptions, conn)

          println(s"Re-Appending the Data to the  ReCreated Oracle Table: $table")

          org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.saveTable(df, url, table, getProps)

        }

      }

    } else {

      println(s"Empty String is passed as Command Line Argument for Table: $table & it seems DataFrame is not required to be Saved in the Oracle DB")

      println(s"If DataFrame is expected to be saved as Oracle Table, provide a valid Oracle Table Name from the Command Line Arguments instead of Null String")

    }

  }

}