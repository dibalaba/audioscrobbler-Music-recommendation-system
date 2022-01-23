 sc.textFile("C:\\Spark\\spark-3.0.3-bin-hadoop2.7\\spark-3.0.3-bin-hadoop2.7\\audioscobbler\\user_artist_data.txt")

 // check the data
 rawUserArtistData.take(3)

 //compute some stats

 rawUserArtistData.map(_.split(' ')(0).toDouble).stats()
 rawUserArtistData.map(_.split(' ')(1).toDouble).stats()

 // map artists' names to ids

 sc.textFile("C:\\Spark\\spark-3.0.3-bin-hadoop2.7\\spark-3.0.3-bin-hadoop2.7\\audioscobbler\\artist_data.txt")

val artistByID= rawArtistData.map { line =>
     | val (id,name)= line.span(_ != '\t')
     | (id.toInt , name.trim)
     | } 

     // let's improve the code

val artistByID = rawArtistData.flatMap { line =>
   val (id,name) = line.span(_ != '\t')
   if (name.isEmpty) {
       None
   } else {
       try{
           Some(id.toInt , name.trim)
       } catch {

           case e: NumberFormatException => None
       }
   }
}

// map aliases with real names
val rawArtistAlias = sc.textFile("C:\\Spark\\spark-3.0.3-bin-hadoop2.7\\spark-3.0.3-bin-hadoop2.7\\audioscobbler\\artist_alias.txt")
val artistAlias = rawArtistAlias.flatMap{ line =>
   val tokens = line.split('\t')
   if (tokens(0).isEmpty) {
       None
   }else {
       Some((tokens(0).toInt , tokens(1).toInt))
   }

}.collectAsMap()

artistByID.lookup(6803336).head
artistByID.lookup(1000010).head

// first model

import org.apache.spark.mllib.recommendation._ 

val bArtistAlias = sc.broadcast(artistAlias)

val trainData = rawUserArtistData.map { line =>
   val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
   val finalArtistID = bArtistAlias.value.getOrElse(artistID , artistID)
   Rating(userID, finalArtistID, count)  
}.cache()

val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)


model.userFeatures.mapValues(_.mkString(" , ")).first()

// model checking
val rawArtistsForUser = rawUserArtistData.map(_.split(' ')).
filter {case Array(user, _ , _) => user.toInt == 2093760}

val existingProducts = rawArtistsForUser.map { case Array(_, artist, _)=> artist.toInt }.
collect().toSet

artistByID.filter { case (id, name) => 
existingProducts.contains(id)
}.values.collect().foreach(println)

val recommendations = model.recommendProducts(2093760, 5)
recommendations.foreach(println) 

val recommendedProductsIDs = recommendations.map( _.product).toSet
 artistByID.filter {case (id,name)=>
  recommendedProductsIDs.contains(id)
 }.values.collect().foreach(println)

 import org.apache.spark.rdd._

 def areaUnderCurve