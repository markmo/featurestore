package diamond

import diamond.load.{CustomerResolverComponent, ParquetDataLoaderComponent}

/**
  * Created by markmo on 10/02/2016.
  */
object ComponentRegistry extends
  ParquetDataLoaderComponent with
  CustomerResolverComponent {

  val dataLoader = new ParquetDataLoader
  val customerResolver = new CustomerResolver

}
