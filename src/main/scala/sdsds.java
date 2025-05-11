//package com.p3.ingestion.poc
//
//import org.apache.spark.sql.functions.{col, length, lit, when, trim}
//import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
//
//import scala.util.Try
//
//object DataPreValidation {
//
//def main(args: Array[String]): Unit = {
//
//val spark = SparkSession.builder().appName("Validation").master("local").getOrCreate()
//
//
//val df = spark.read.parquet("/home/p3/Pictures/ParquetFiles/parquet/DBO/ADDRESS")
//
////    val clone_df = df.clone()
//
//val constraintsList = List(
//        ConstraintPreValidationBean("ADDRESS", "STRING", "NOTNULL"),
//        ConstraintPreValidationBean("ADDRESS", "STRING", "BOOLEAN"),
//        ConstraintPreValidationBean("ADDRESS", "STRING", "UNIQUE"),
//        ConstraintPreValidationBean("ADDRESS", "STRING", "LENGTH", operator = Some("="), value = Some("7")),
////      ConstraintPreValidationBean("ADDRESS", "STRING", "RANGE", minValue = Some("1"), maxValue = Some("100")),
//        ConstraintPreValidationBean("ADDRESS", "STRING", "DATATYPE")
//)
//
//val groupedConstraints = constraintsList.groupBy(_.columnName)
//
//val validatedDf = groupedConstraints.foldLeft(df) { (tempDf, columnConstraints) =>
//applyValidations(tempDf, columnConstraints._2)
//    }
//
//            validatedDf.show()
//
//  }
//
//private def validateDataType(df: DataFrame, colName: String, expectedType: String): Column = {
//val actualType = df.schema(colName).dataType.typeName
//    if (actualType == expectedType.toLowerCase) {
//lit(colName)
//    } else {
//lit(null)
//    }
//            }
//
//private def validateNotNull(df: DataFrame, colName: String): Column = {
//when(df(colName).isNotNull, colName).otherwise(null)
//  }
//
//private def validateBoolean(df: DataFrame, colName: String): Column = {
//when(df(colName).cast("boolean").isin(true, false), colName).otherwise(null)
//  }
//
//private def validateUnique(df: DataFrame, colName: String): Column = {
//val isUnique = df.select(colName).distinct().count() == df.count()
//when(lit(isUnique), colName).otherwise(null)
//  }
//
//private def applyValidations(df: DataFrame, constraints: List[ConstraintPreValidationBean]): DataFrame = {
//var resultDf = df
//
//    constraints.foreach { constraint =>
//        val colName = constraint.columnName
//    val validationCol = constraint.validationType match {
//        case "DATATYPE" => validateDataType(df, colName, constraint.dataType)
//        case "NOTNULL" => validateNotNull(df, colName)
//        case "BOOLEAN" => validateBoolean(df, colName)
//        case "UNIQUE" => validateUnique(df, colName)
//        case "LENGTH" => validateLength(colName, constraint)
//        case "RANGE" => validateRange(colName, constraint)
//        case _ => lit(null)
//    }
//    resultDf = resultDf.withColumn(s"${colName}_${constraint.validationType}", validationCol)
//}
//resultDf
//  }
//
//
//private def validateLength(colName: String, constraint: ConstraintPreValidationBean): Column = {
//val lengthValue = Try(constraint.value.getOrElse("0").toInt).getOrElse(0)
//
//val lengthCondition = constraint.operator match {
//    case Some("=") => length(trim(col(colName))) === lengthValue
//    case Some("!=") => length(trim(col(colName))) =!= lengthValue
//    case Some(">") => length(trim(col(colName))) > lengthValue
//    case Some("<") => length(trim(col(colName))) < lengthValue
//    case Some(">=") => length(trim(col(colName))) >= lengthValue
//    case Some("<=") => length(trim(col(colName))) <= lengthValue
//    case _ => lit(false)
//}
//
//when(lengthCondition, lit(true)).otherwise(lit(false))
//        }
//
//private def validateRange(colName: String, constraint: ConstraintPreValidationBean): Column = {
//val minValue = Try(constraint.minValue.getOrElse(Double.MinValue.toString).toDouble).getOrElse(Double.MinValue)
//val maxValue = Try(constraint.maxValue.getOrElse(Double.MaxValue.toString).toDouble).getOrElse(Double.MaxValue)
//
//val numericCol = when(col(colName).rlike("^-?\\d+(\\.\\d+)?$"), col(colName).cast("double"))
//
//val rangeCondition = numericCol.between(minValue, maxValue)
//
//when(rangeCondition, lit(true)).otherwise(lit(false))
//        }
//
//        }
