import org.apache.spark.sql._
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





    //      val pdfBytes = "%PDF-1.4".getBytes("UTF-8")
    //      val fakeBytes = "random".getBytes("UTF-8")
    //      val testDF = spark.createDataFrame(Seq(
    //        (1, pdfBytes),
    //        (2, fakeBytes)
    //      )).toDF("id", "file_blob")
    //      testDF.show()
    //
    //      val detectFormat = udf { bytes: Array[Byte] =>
    //        if (bytes == null || bytes.length < 4) "unknown"
    //        else {
    //          val hex = bytes.take(4).map("%02X".format(_)).mkString
    //          hex match {
    //            case h if h.startsWith("25504446") => "pdf"
    //            case h if h.startsWith("FFD8FF")   => "jpeg"
    //            case h if h.startsWith("89504E47") => "png"
    //            case h if h.startsWith("504B0304") => "zip"
    //            case _ => "unknown"
    //          }
    //        }
    //      }
    //      val dfWithFormat = testDF.withColumn("detected_format", detectFormat(col("file_blob")))
    //      dfWithFormat.show()
    //      val invalidFiles = dfWithFormat
    //        .filter(col("detected_format").isNull
    //          || col("detected_format") =!= "pdf")
    //        .limit(1)
    //        .count() > 0
    //
    //      if (invalidFiles) {
    //        invalidColumns += (("file_blob", "Invalid format: not a PDF"))
    //      }
  }
