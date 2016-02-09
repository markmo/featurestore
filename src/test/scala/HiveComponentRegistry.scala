import diamond.load.HiveDataLoaderComponent

/**
  * Created by markmo on 10/02/2016.
  */
object HiveComponentRegistry extends HiveDataLoaderComponent {

  val dataLoader = new HiveDataLoader

}
