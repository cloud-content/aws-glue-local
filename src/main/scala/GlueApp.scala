import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

object GlueApp {
  def main(sysArgs: Array[String]) {
    System.setProperty("spark.app.name", "GlueJob")
    System.setProperty("spark.master","local[4]")
    val sc: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sc)
    val spark: SparkSession = glueContext.getSparkSession
    import spark.implicits._

    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .load("src/main/resources/employees.csv")  // load() returns a DataFrame
    df.printSchema()
    df.show(10, false)
    Job.commit()
  }
}
