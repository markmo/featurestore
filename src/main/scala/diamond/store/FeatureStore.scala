package diamond.store

import diamond.models.Feature

import scala.collection.mutable

/**
  * Created by markmo on 30/11/2015.
  */
class FeatureStore {

  val registeredFeatures = mutable.ArrayBuffer[Feature]()

  def registerFeature(feature: Feature) =
    if (!registeredFeatures.contains(feature)) {
      registeredFeatures += feature
    }

}
