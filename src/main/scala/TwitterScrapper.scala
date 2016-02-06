import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContextState
import org.apache.spark.streaming.twitter.TwitterUtils
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.Imports._
import com.typesafe.config.ConfigFactory
import java.io.FileOutputStream
import java.io.File
import java.io.PrintWriter
import scala.collection.mutable.{Map}

object TwitterScrapper {  
  def log(message: String, level: String) = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val moment = formatter.format(new Date())
    
    val line = moment + " " + level + ": " + message;
    
    val f = new File("logs/social_media.log");
    
    var writer: PrintWriter = null;
    if ( f.exists() && !f.isDirectory() ) {
        writer = new PrintWriter(new FileOutputStream(new File("logs/social_media.log"), true));
        writer.append(line + "\n");
        writer.close();
    }
    else {
        f.getParentFile().mkdirs();
        writer = new PrintWriter("logs/social_media.log");
        writer.println(line);
        writer.close();
    }
    
  }
  
  def main(args: Array[String]) {    
    val config =  ConfigFactory.parseFile(new File("./application.conf"))
    // Configure Twitter credentials
    TwitterCredentials.configureTwitterCredentials()

    // Your code goes here
    val conf = new SparkConf().setAppName("TwitterScrapper")
    val ssc = new StreamingContext(conf, Seconds(5))
    
    var tweets = TwitterUtils.createStream(ssc, None)
    var statuses = tweets.map { status => (status.getText, status.getCreatedAt, status.getLang) }
    statuses = statuses.filter { status => status._3 == "es" || status._3 == "en" }
    
    val brandName = config.getString("app.brand-name").replaceAll("[~!@#$^%&*\\(\\)_+={}\\[\\]|;:\"'<,>.?`/\\\\-]", "").toLowerCase();
    val productName = config.getString("app.product-name").replaceAll("[~!@#$^%&*\\(\\)_+={}\\[\\]|;:\"'<,>.?`/\\\\-]", "").toLowerCase();
    
    val brandStatuses = statuses.filter { status =>  status._1.toLowerCase().matches("\\b" + brandName + "\\b") || status._1.toLowerCase().matches("\\b" + productName + "\\b") }
    
    var brandSum = 0L
    var brandCount = 0.0
    
    brandStatuses.count().foreachRDD((rdd, time) => {
      val prev_avg = if(brandCount == 0.0) 0.0 else brandSum / brandCount;
      brandSum += rdd.first()
      brandCount += 1.0
      
      val curr_avg = brandSum / brandCount
      
      val increaseRate = if(prev_avg == 0.0) 0.0 else (curr_avg - prev_avg) * 100;
      
      val mongoBrandRuleConnection = MongoClient();
      var mongoBrandRuleColl = mongoBrandRuleConnection("Acme-Supermarket")("social_media_rules");
      
      val query = MongoDBObject("_type" -> "BrandRule", "increaseRate" -> MongoDBObject( "$lte" -> increaseRate));
      val fields = MongoDBObject("_id" -> 1);
      val brandRules = mongoBrandRuleColl.find(query, fields);
      
      brandRules.foreach { rule => 
        val mongoBrandNotificationConnection = MongoClient();
        var mongoBrandNotificationColl = mongoBrandNotificationConnection("Acme-Supermarket")("social_media_notifications");
      
        val brandNotification = MongoDBObject(
          "_type" -> "BrandNotification",
          "percentageExceeded" -> increaseRate,
          "brand_rule_id" -> rule.get("_id").toString(),
          "moment" -> new Date()
        )
        
        mongoBrandNotificationColl += brandNotification;
        
        log("Brand notification for rule" + rule.get("_id").toString(), "INFO");
        
        mongoBrandNotificationConnection.underlying.close();
        mongoBrandNotificationColl = null;
      }
      
      mongoBrandRuleConnection.underlying.close();
      mongoBrandRuleColl = null;
    })
    
    brandStatuses.foreachRDD( brandRDD => {
      brandRDD.foreach(brand => {
        val mongoBrandDataConnection = MongoClient();
        var mongoBrandDataColl = mongoBrandDataConnection("Acme-Supermarket")("social_media_brand_data");
        
        val brandOcurrence = MongoDBObject(
            "date" -> new Date(),
            "description" -> brand._1
        );
        
        mongoBrandDataColl += brandOcurrence;
        
        mongoBrandDataConnection.underlying.close();
        mongoBrandDataColl = null;
        
        log("Ocurrencia de marca", "INFO");
      })
    });
    
    val product_data = Map[Integer, Map[String, Any]]();
    
    val mongoProductRuleConnection = MongoClient();
    var mongoProductRuleColl = mongoProductRuleConnection("Acme-Supermarket")("social_media_rules");
    
    val query = MongoDBObject("_type" -> "ProductRule");
    val fields = MongoDBObject("_id" -> 1, "product_id" -> 1);
    val productRules = mongoProductRuleColl.find(query, fields);
    
    val product_ids = productRules.map { rule => Integer.parseInt(rule.get("product_id").toString()) }.toList
   
    val mongoProductConnection = MongoClient();
    var mongoProductColl = mongoProductConnection("Acme-Supermarket")("products");
            
    val ruled_products = mongoProductColl.find("_id" $in product_ids);
   
    val stopWords = new StopWords();
    
    ruled_products.foreach { product => 
      val p_id = Integer.parseInt(product.get("_id").toString());
      var p_name = product.get("name").toString().toLowerCase().split(" ");
      
      p_name = stopWords.filter_stopwords(p_name, Languages.Any);
      
      val productStatuses = statuses.filter(status => p_name.exists(word => status._1.matches(".*\\b" + word.replaceAll("[~!@#$^%&*\\(\\)_+={}\\[\\]|;:\"'<,>.?`/\\\\-]", "") + "\\b.*")) )
      
      productStatuses.count().foreachRDD((rdd, time) => {
        var prev_avg = 0.0;
        var curr_avg = 0.0;
        
        if(product_data.contains(p_id)) {
          val product_values = product_data.get(p_id).get;
          var product_sum = product_values.get("sum").get.asInstanceOf[Long];
          var product_count = product_values.get("count").get.asInstanceOf[Double];
          
          prev_avg = product_sum / product_count;
          
          product_sum += rdd.first();
          product_count += 1.0;
          
          curr_avg = product_sum / product_count;
          
          product_values.put("sum", product_sum);
          product_values.put("count", product_count);
          
          product_data.put(p_id, product_values);
        } else {
          val product_values = Map[String, Any]();
          val product_sum = rdd.first();
          val product_count = 1.0;
          
          curr_avg = product_sum / product_count;
          
          product_values.put("sum", product_sum);
          product_values.put("count", product_count);
          
          product_data.put(p_id, product_values);
        }
        
        val increaseRate = if(prev_avg == 0.0) 0.0 else (curr_avg - prev_avg) * 100;
        
        val mongoProductRuleConnection = MongoClient();
        var mongoProductRuleColl = mongoProductRuleConnection("Acme-Supermarket")("social_media_rules");
        
        val query = MongoDBObject("_type" -> "ProductRule", "increaseRate" -> MongoDBObject( "$lte" -> increaseRate));
        val fields = MongoDBObject("_id" -> 1);
        val productRules = mongoProductRuleColl.find(query, fields);
        
        productRules.foreach { rule => 
          val mongoProductNotificationConnection = MongoClient();
          var mongoProductNotificationColl = mongoProductNotificationConnection("Acme-Supermarket")("social_media_notifications");
        
          val productNotification = MongoDBObject(
            "_type" -> "ProductNotification",
            "percentageExceeded" -> increaseRate,
            "product_rule_id" -> rule.get("_id").toString(),
            "moment" -> new Date()
          )
          
          mongoProductNotificationColl += productNotification;
          
          log("Product notification for rule" + rule.get("_id").toString(), "INFO");
          
          mongoProductNotificationConnection.underlying.close();
          mongoProductNotificationColl = null;
        }
        
        mongoProductRuleConnection.underlying.close();
        mongoProductRuleColl = null;
      });
    }
    
    statuses.foreachRDD( statusRDD => {
      statusRDD.foreach( status => {
        var stopWords = new StopWords();
        //println(status._1);
        
        val mongoProductConnection = MongoClient();
        var mongoProductColl = mongoProductConnection("Acme-Supermarket")("products");
        
        val mongoProductDataConnection = MongoClient();
        var mongoProductDataColl = mongoProductDataConnection("Acme-Supermarket")("social_media_product_data");
        
        //Por cada producto, mirar si el tweet contiene al nombre del mismo y, en caso afirmativo, 
        //meter una entrada en la tabla SocialMediaProductData con el texto y la fecha del tweet y la ID del producto        
        val query = MongoDBObject.empty;
        val fields = MongoDBObject("_id" -> 1, "name" -> 1);
        val products = mongoProductColl.find(query, fields);
                
        products.foreach { product => {
          val p_id = product.get("_id");
          val p_name = product.get("name").toString().toLowerCase().replaceAll("[~!@#$^%&*\\(\\)_+={}\\[\\]|;:\"'<,>.?`/\\\\-]", "");
          
          if(status._1.toLowerCase().matches(".*\\b" + p_name + "\\b.*")) {
            val productOcurrence = MongoDBObject(
              "date" -> new Date(),
              "description" -> status._1,
              "product_id" -> p_id
            );
            
            mongoProductDataColl += productOcurrence;
            log("Ocurrencia de producto", "INFO");
          }
        }}
        
        //Clean-up
        mongoProductConnection.underlying.close();
        mongoProductColl = null;
        
        mongoProductDataConnection.underlying.close();
        mongoProductDataColl = null;
        
        stopWords = null;
      });
    });
    
    sys.addShutdownHook({
      println("ShutdownHook called")
      ssc.stop(true, true)

      while (!ssc.getState().equals(StreamingContextState.STOPPED)) {
        println("Stopping... - " +  ssc.getState().toString())
      }
    })
    
    ssc.start()   
    ssc.awaitTermination()
  }
}