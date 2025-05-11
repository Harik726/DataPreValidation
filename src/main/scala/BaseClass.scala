import java.util.UUID

case class BaseClass(columnName:String,
                     columnID:UUID,
                     tableId:UUID,
                     dataType:String,
                     dataLength:Long,
                     maxLength:Long,
                     minLength:Long,
                     dataValidationBean:DataValidationBean) {

}
