package de.hpi.spark_tutorial
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.log4j.Logger
import org.apache.log4j.Level

// https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/publications/2015/Scaling_out_the_discovery_of_INDs-CR.pdf

object SimpleSpark extends App {
  override def main(args: Array[String]): Unit = {
    // command line parameter parsing
    var path = "./TPCH"
    var cores = "4"
    var i = 0
    while(i < args.length) {
      if(args(i) == "--path") {
        path = args(i+1)
        i += 1
      }
      else if(args(i) == "--cores"){
        cores = args(i+1)
        i += 1
      }
      i += 1
    }

    println(s"Using path: $path")
    println(s"Number of cores: $cores")

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master(s"local[$cores]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    // define all inputs
    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
      .map(name => s"$path/tpch_$name.csv")

    // Read a Dataset from a file
    val tables = inputs.map(filename => { spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter",";")
      .csv(filename) })

    // for each table, take the column names and with it the corresponding column, delete duplicate elements per column
    val columns = tables.flatMap(table => { table.columns.map(colName => { table.select(colName).distinct() })} )

    // for each column, take each cell and create pairs of the cell value and its column name
    val valueColumnPairs = columns.map(col => col.map(cell => (cell.get(0).toString, cell.schema.names(0) )) )

    // take the list of lists of pairs and append them to one another to get one large list
    val reducedColumnPairs = valueColumnPairs.reduce((c1, c2) => c1.union(c2)).rdd

    // group all the pairs by the first element (combining all pairs of each first element to one pair) and only keep its list of columns (+ convert to set)
    val attribute_sets = reducedColumnPairs.groupByKey().map(_._2.toSet)

    // for each of those column sets, take each single column and create a new pair of this column and all other columns from the set, discard duplicates
    val inclusionLists = attribute_sets.flatMap(set => set.map(col => (col, set.filter(otherCol => !otherCol.equals(col))))).distinct()

    // for each distinct first element of the pairs, get a list of all the second elements through reduceByKey and intersect all of them pairwise, only keep non-empty pairs
    val aggregates = inclusionLists.reduceByKey((refs1, refs2) => refs1.intersect(refs2)).filter(_._2.nonEmpty)

    // for each of the final pairs, reformat them to a single string and sort
    val output = aggregates.collect().map(element => element._1.concat(" < ").concat(element._2.mkString(", "))).sorted // ⊆

    // output the results
    output.foreach(println)
  }
}
