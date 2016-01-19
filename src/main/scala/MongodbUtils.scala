import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.stratio.datasource._
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.schema._
import com.stratio.datasource.mongodb.writer._
import org.apache.spark.sql.SQLContext
import Config._
import MongodbConfig._
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.MongoClient

object MongodbUtils {
  def connect(builder: MongodbConfigBuilder, collection_name: String, sc:SparkContext) : SQLContext = { 
      val readConfig = builder.build();
      val sqlContext = new SQLContext(sc)
      
      val mongoRDD = sqlContext.fromMongoDB(readConfig);
      mongoRDD.registerTempTable(collection_name);
      
      return sqlContext;
      
  }
  
  def getBuilder(db_name:String, collection_name:String) : MongodbConfigBuilder = {
    val builder = MongodbConfigBuilder(Map(Host -> List("localhost:27017"), Database -> "Acme-Supermarket", 
                                              Collection -> "products", SamplingRatio -> 1.0, 
                                              WriteConcern -> MongodbWriteConcern.Normal));
    
    return builder;
  }
  
  def createDataFrame(data:RDD[Row], schema: StructType, sql:SQLContext) : DataFrame = {    
    var dataFrame = sql.createDataFrame(data, schema);
    
    return dataFrame;
  }
}