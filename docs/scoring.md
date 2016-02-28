## Scoring

Functions to score Spark data structures with H2O POJO models. See https://github.com/h2oai/h2o-3/blob/master/h2o-docs/src/product/howto/POJO_QuickStart.md

Assume pojo model is "model.java" and model name is "model_name".

Step 1: Package pojo model into jar (requires h2o-genmodel.jar): javac -cp h2o-genmodel.jar -J-Xmx2g -J-XX:MaxPermSize=256m model.java jar -cf model.jar *.class

Step 2: Include model.jar in Spark instance: export SPARK_SUBMIT_OPTIONS="--driver-class-path .../sparkling-water-1.5.6/assembly/build/libs/sparkling-water-assembly-1.5.6-all.jar \ --jars .../sparkling-water-1.5.6/assembly/build/libs/sparkling-water-assembly-1.5.6-all.jar,.../model.jar

Step 3: In Spark, load model by reflection:

    import hex.genmodel.GenModel
    val model = Class.forName("<model-name>").newInstance().asInstanceOf[GenModel]
