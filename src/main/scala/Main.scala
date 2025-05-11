import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, concat, date_format, length, lit, log, to_date, to_timestamp, trim, when}
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, IntegerType, StringType}
import scala.collection.mutable.ListBuffer

  object Main {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().appName("Validation").master("local").getOrCreate()
     val columnMetaDatas : List [BaseClass] = List(
        BaseClass(
          columnName = "TIME",
          columnID = null,
          tableId = null,
          dataType = "date",
          dataLength = 2,
          minLength=4,
          maxLength=5,
          dataValidationBean = DataValidationBean(
            nullCheck = false,
            typecheck = false,
            legthCheck = false,
            dateCheck = false,
            booleanCheck = false,
            fileFormatCheck = false,
            rangeCheck = true,
            uniqueCheck = true
          )
        ),
       BaseClass(
         columnName = "City",
         columnID = null,
         tableId = null,
         dataType = "date",
         dataLength = 2,
         minLength=4,
         maxLength=5,
         dataValidationBean = DataValidationBean(
           nullCheck = false,
           typecheck = false,
           legthCheck = false,
           dateCheck = false,
           booleanCheck = false,
           fileFormatCheck = false,
           rangeCheck = false,
           uniqueCheck = true
         )
       )
     )

     val columnMetaData=ColumnMetaData(
        columnMetaData = columnMetaDatas
      )

//      val df = spark.read.parquet("/Users/k7/Downloads/claim_sys/DBO/CLAIM")
      val df = spark.read.parquet("/Users/k7/Downloads/test")
      df.show()
      val invalidColumns = ListBuffer.empty[(String, String)] // mutable list
      val dataValidationImp= new DataValidationImp


      columnMetaData.columnMetaData.foreach { row =>
        val originalColumnName = row.columnName
        dataValidationImp.validateColumnTypes(df, row, invalidColumns)
        dataValidationImp.validateNullOrEmpty(df, row, invalidColumns)
        dataValidationImp.validateUniqueData(df, row, invalidColumns)
         dataValidationImp.validateLength(df, row, invalidColumns)
         dataValidationImp.validateDate(df, row, invalidColumns, spark)
         dataValidationImp.validateBoolean(df, row, invalidColumns)
         dataValidationImp.validateRange(df, row, invalidColumns)
      }

      if (invalidColumns.nonEmpty) {
        println("Validation failed for the following columns:")
        invalidColumns.foreach { case (colName, reason) =>
          println(s" - $colName: $reason")
        }
      } else {
        println("All columns passed validation.")
      }
    }

  }
