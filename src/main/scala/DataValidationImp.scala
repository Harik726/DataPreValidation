import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer


class DataValidationImp {

  def validateColumnTypes(df: DataFrame, row: BaseClass, invalidColumns: ListBuffer[(String, String)]): ListBuffer[(String, String)] = {
    val originalColumnName = row.columnName
    if (row.dataValidationBean.typecheck) {
      val actualType = df.schema(originalColumnName).dataType.typeName
      if (actualType != row.dataType) {
        invalidColumns += ((originalColumnName, s"Type mismatch: expected '${row.dataType}', found '$actualType'"))
      }
    }
    invalidColumns
  }


  def validateNullOrEmpty(df: DataFrame, row: BaseClass, invalidColumns: ListBuffer[(String, String)]): ListBuffer[(String, String)] = {
    val originalColumnName = row.columnName
    if (row.dataValidationBean.nullCheck) {
      val hasNullOrEmpty = df
        .filter(col(originalColumnName).isNull || trim(col(originalColumnName)) === "")
        .limit(1)
        .count() > 0
      if (!hasNullOrEmpty) {
        invalidColumns += ((originalColumnName, "Doesn't Contains null or empty values"))
      }
    }
    invalidColumns
  }

  def validateUniqueData(df: DataFrame, row: BaseClass, invalidColumns: ListBuffer[(String, String)]): ListBuffer[(String, String)] = {
    val originalColumnName = row.columnName
    if (row.dataValidationBean.uniqueCheck) {
      val totalCount = df.count()
      val distinctCount = df.select(col(originalColumnName)).distinct().count()
      if (totalCount != distinctCount) {
        invalidColumns += ((originalColumnName, "Column contains duplicate values"))
      }
    }
    invalidColumns
  }

  def validateLength(df: DataFrame, row: BaseClass, invalidColumns: ListBuffer[(String, String)]): ListBuffer[(String, String)] = {
    val originalColumnName = row.columnName
    if (row.dataValidationBean.legthCheck) {
      val hasInvalidLengthValid = df
        .filter(col(originalColumnName).isNull
          || length(trim(col(originalColumnName))) =!= row.dataLength)
        .limit(1)
        .count() > 0
      if (hasInvalidLengthValid) {
        invalidColumns += ((originalColumnName, s"Length mismatch: expected '${row.dataLength}' characters"))
      }
    }
    invalidColumns
  }

  def validateDate(df: DataFrame, row: BaseClass, invalidColumns: ListBuffer[(String, String)], spark: SparkSession): ListBuffer[(String, String)] = {
    val originalColumnName = row.columnName
    if (row.dataValidationBean.dateCheck) {
      spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
      val dateFormat = "yyyy-MM-dd"
      val dateTimeFormat = "yyyy-MM-dd HH:mm:ss"
      val trimmedCol = trim(col(originalColumnName))
      val isInValidDateOrDateTime = df
        .filter(col(originalColumnName).isNull ||
          trimmedCol.isNotNull &&
            !(
              (
                to_date(trimmedCol, dateFormat).isNotNull &&
                  date_format(to_date(trimmedCol, dateFormat), dateFormat) === trimmedCol
                ) || (
                to_timestamp(trimmedCol, dateTimeFormat).isNotNull &&
                  date_format(to_timestamp(trimmedCol, dateTimeFormat), dateTimeFormat) === trimmedCol
                )
              )
        )
        .limit(1)
        .count() > 0

      if (isInValidDateOrDateTime) {
        invalidColumns += ((originalColumnName, "Invalid date or datetime format"))
      }
    }
    invalidColumns
  }

  def validateBoolean(df: DataFrame, row: BaseClass, invalidColumns: ListBuffer[(String, String)]): ListBuffer[(String, String)] = {
    val originalColumnName = row.columnName
    if (row.dataValidationBean.booleanCheck) {
      if (df.schema(originalColumnName).dataType.typeName == "boolean") {
        val hasInValidBoolean = df
          .filter(col(originalColumnName).isNull
            || !col(originalColumnName).isin(true, false))
          .limit(1)
          .count() > 0
        if (hasInValidBoolean) {
          invalidColumns += ((originalColumnName, "Invalid boolean value (should be true or false)"))
        }
      }
      else {
        invalidColumns += ((originalColumnName, s"Expected boolean type, found '${df.schema(originalColumnName).dataType.typeName}'"))
      }
    }
    invalidColumns
  }


  def validateRange(df: DataFrame, row: BaseClass, invalidColumns: ListBuffer[(String, String)]): ListBuffer[(String, String)] = {
    val originalColumnName = row.columnName
    if (row.dataValidationBean.rangeCheck) {
      val hasInValidRange = df
        .filter(col(originalColumnName).isNull ||
          length(trim(col(originalColumnName))) < row.minLength ||
          length(trim(col(originalColumnName))) > row.maxLength
        )
        .limit(1)
        .count() > 0

      if (hasInValidRange) {
        invalidColumns += (
          (originalColumnName, s"Length out of range: expected between ${row.minLength} and ${row.maxLength}"))
      }
    }
    invalidColumns
  }


//  def validateFileFormat(df: DataFrame, row: BaseClass, invalidColumns: ListBuffer[(String, String)]): ListBuffer[(String, String)] = {
//    val originalColumnName = row.columnName
//    val detectFormat = udf { bytes: Array[Byte] =>
//      if (bytes == null || bytes.length < 4) "unknown"
//      else {
//        val hex = bytes.take(4).map("%02X".format(_)).mkString
//        hex match {
//          case h if h.startsWith("25504446") => "pdf"
//          case h if h.startsWith("FFD8FF")   => "jpeg"
//          case h if h.startsWith("89504E47") => "png"
//          case h if h.startsWith("504B0304") => "zip"
//          case _ => "unknown"
//        }
//      }
//    }
//    val dfWithFormat = df.withColumn("detected_format", detectFormat(col("file_blob")))
//    dfWithFormat.show()
//    val invalidFiles = dfWithFormat
//      .filter(col("detected_format").isNull
//        || col("detected_format") =!= "pdf")
//      .limit(1)
//      .count() > 0
//
//    if (invalidFiles) {
//      invalidColumns += (("file_blob", "Invalid format: not a PDF"))
//    }
//    invalidColumns
//  }


  }
