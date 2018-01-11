package spark.examples

import org.apache.spark._
import java.io.PrintWriter

object FirstLine {
	def main(args: Array[String]) = {
		val conf = new SparkConf().setAppName("Spark Word Counter").setMaster("local")
		val spark = new SparkContext(conf)
		val data = spark.textFile("/tmp/data.csv")
		val dataWithIndex = data.zipWithIndex
		dataWithIndex.foreach { line =>
			if (line._2 == 0) {
				new PrintWriter("/tmp/output.txt") { write(line._1); close }
			}
		}
		spark.stop()
	}
}
