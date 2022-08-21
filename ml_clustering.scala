import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

val schema= StructType(Array(StructField("sepal_length",DoubleType,true),StructField("sepal_width",DoubleType,true),StructField("petal_length",DoubleType,true),StructField("petal_width",DoubleType,true),StructField("species",StringType,true)))
val dataset= spark.read.format("csv").option("delimiter",",").option("header","false").schema(schema).load("iris")
val assembler = new VectorAssembler().setInputCols(Array("sepal_length","sepal_width","petal_length","petal_width")).setOutputCol("features")
val ds= assembler.transform(dataset).select("features")
// Creacion y entrenamiento
val kmeans = new KMeans().setK(3).setSeed(1L)
val model = kmeans.fit(ds)
// Prediccion
val predictions = model.transform(ds)
// Evalua - score de Silueta
val evaluator = new ClusteringEvaluator()
val silhouette = evaluator.evaluate(predictions)
println(s"Silueta con el cuadrado de la dist euclidiana= $silhouette")
// Muestra los resultados
println("Posicion de los centroides: ")
model.clusterCenters.foreach(println)

val arrPredict= predictions.select("prediction").collect()
var arrSpecies = dataset.select("species").collect()

val rowsRes= dataset.rdd.zip(predictions.select("prediction").rdd).map{case(rowLeft,rowRight) => Row.fromSeq(rowLeft.toSeq ++ rowRight.toSeq)}
val schemaPred= StructType(Array(StructField("predicted",IntegerType,true)))
val schemaRes= StructType(schema.fields ++ schemaPred.fields)
val dsRes: DataFrame= spark.createDataFrame(rowsRes,schemaRes)
dsRes.groupBy("species","predicted").count().show()
