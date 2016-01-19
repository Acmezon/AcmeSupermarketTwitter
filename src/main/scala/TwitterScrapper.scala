

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContextState
import org.apache.spark.streaming.twitter.TwitterUtils
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import java.util.logging.Logger
import java.util.logging.Level

object TwitterScrapper {  
  def log(message: String, level: String) : String = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val moment = formatter.format(new Date())
    
    return moment + " " + level + ": " + message;
  }
  
  def main(args: Array[String]) {    
    // Configure Twitter credentials
    TwitterCredentials.configureTwitterCredentials()

    // Your code goes here
    val conf = new SparkConf().setAppName("TwitterScrapper")
    val ssc = new StreamingContext(conf, Seconds(5))    
      
    sys.addShutdownHook({
      println("ShutdownHook called")
      ssc.stop(true, true)

      while (!ssc.getState().equals(StreamingContextState.STOPPED)) {
        println("Stopping... - " +  ssc.getState().toString())
      }
    })
    
    var tweets = TwitterUtils.createStream(ssc, None)
    var statuses = tweets.map { status => (status.getText, status.getCreatedAt, status.getLang) }
    statuses = statuses.filter { status => status._3 == "es" || status._3 == "en" }
     
    statuses.foreachRDD( statusRDD => {
      statusRDD.foreach( status => {
        var log = Logger.getLogger("Acme-Supermarket Twitter Logger");
        var stopWords = new StopWords();
        //println(status._1);
        
        val mongoProductConnection = MongoClient();
        var mongoProductColl = mongoProductConnection("Acme-Supermarket")("products");
        
        val mongoBrandDataConnection = MongoClient();
        var mongoBrandDataColl = mongoBrandDataConnection("Acme-Supermarket")("SocialMediaBrandData");
        
        val mongoProductDataConnection = MongoClient();
        var mongoProductDataColl = mongoBrandDataConnection("Acme-Supermarket")("SocialMediaProductData");
        
        val brandContained = status._1.toLowerCase().contains("amazon");
        val productContained = status._1.toLowerCase().contains("amazon dash");
        
        if(brandContained || productContained) {
          //Introducir una entrada en la tabla de SocialMediaBrandData con el texto y la fecha
          val brandOcurrence = MongoDBObject(
              "date" -> (status._2.getTime / 1000L).toString(),
              "description" -> status._1
          );
          
          mongoBrandDataColl += brandOcurrence;
          
          log.log(Level.FINE, "Ocurrencia de marca encontrada");
            println("Ocurrencia de marca");
        }
        
        //Por cada producto, mirar si el tweet contiene al nombre del mismo y, en caso afirmativo, 
        //meter una entrada en la tabla SocialMediaProductData con el texto y la fecha del tweet y la ID del producto        
        val query = MongoDBObject.empty;
        val fields = MongoDBObject("_id" -> 1, "name" -> 1);
        val products = mongoProductColl.find(query, fields);
                
        products.foreach { product => {
          val p_id = product.get("_id");
          var p_name = product.get("name").toString().toLowerCase().split(" ");
          
          p_name = stopWords.filter_stopwords(p_name, Languages.Any)
          
          if(p_name.exists(status._1.contains)) {
            val productOcurrence = MongoDBObject(
              "date" -> (status._2.getTime / 1000L).toString(),
              "description" -> status._1,
              "product_id" -> p_id
            );
            
            mongoProductDataColl += productOcurrence;
            log.log(Level.FINE, "Ocurrencia de producto");
            println("Ocurrencia de producto");
          }
        }}
        
        //Clean-up
        mongoProductConnection.underlying.close();
        mongoProductColl = null;
        
        mongoBrandDataConnection.underlying.close();
        mongoBrandDataColl = null;
        
        mongoProductDataConnection.underlying.close();
        mongoProductDataColl = null;
        
        log = null;
        stopWords = null;
      });
    });
    
    ssc.start()   
    ssc.awaitTermination()
  }
}