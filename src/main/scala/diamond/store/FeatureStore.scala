package diamond.store

import diamond.models.AttributeType

import scala.collection.mutable

/**
  * Created by markmo on 30/11/2015.
  */
class FeatureStore {

  val registeredFeatures = mutable.ArrayBuffer[AttributeType]()

  def registerFeature(feature: AttributeType) =
    if (!registeredFeatures.contains(feature)) {
      registeredFeatures += feature
    }

}
