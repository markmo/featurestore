package diamond.store

import diamond.AppConfig

/**
  * Created by markmo on 12/03/2016.
  */
trait Repository extends Serializable {

  val conf: AppConfig

}
