// Import classes

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Encoders
import org.apache.log4j.{Logger, Level}

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{FileSplit, TextInputFormat}
import org.apache.spark.rdd.HadoopRDD

object DataProcessor {

	def main (args: Array[String]) {

		val appName = "Big Data Architecture - Project 1"
		val master = "local"

		// Initialize SparkContext
		val conf = new SparkConf().setAppName(appName).setMaster(master)
		val sc = new SparkContext(conf)

		val sqlContext= new org.apache.spark.sql.SQLContext(sc) 
		import sqlContext.implicits._ 

		// Path name to data (viewed from base directory of application)
		val directory = "../data/"

		// 
		val reviewFileName = "yelp_top_reviewers_with_reviews.csv.gz"
		val businessFileName = "yelp_businesses.csv.gz"
		val friendshipFileName = "yelp_top_users_friendship_graph.csv.gz"

// -------------------------------------------------------------------------------------------- //

	// Task 1: Create RDDs from the data and count the records

		val reviewTableRaw = sc.textFile(directory + reviewFileName)
		println("Review Table count: " + reviewTableRaw.count())

		val businessTableRaw = sc.textFile(directory + businessFileName)	
		println("Business Table count: " + businessTableRaw.count())

		val friendshipGraphRaw = sc.textFile(directory + friendshipFileName)	
		println("Friendship Graph count: " + friendshipGraphRaw.count())

// --------------------------------------REVIEW TABLE------------------------------------------ //

	// Task 2.a: Distinct users in dataset

		// Initially we split each string on each row into a list
		// and remove the header form the dataset
		// We also decode the base64 review_text and remove the '\n' characters
		// TODO: decode unixtimestamp (field 4)
		val header = reviewTableRaw.first()
		val reviewTable = reviewTableRaw.filter(line => line != header).map(line => line.split("\t"))
				.map(record => List(record(0),record(1),record(2),
					(new String(java.util.Base64.getMimeDecoder().decode(record(3)))).filter(_ >= ' ')
					,record(4)))

		// Map the RDD on a new RDD and only keep the user_id column, the distinct function only takes the unique rows
		val reviewTableOnlyUsersDistinct = reviewTable.map(record => record(1)).distinct()
		println("Distinct users: " + reviewTableOnlyUsersDistinct.count())

	// Task 2.b: Average number of characters in user review

		// We first replace the row with the length of the review (without spaces)
		// Then we convert the RDD into a DataFrame in order to calculate the average length
		val reviewLengthDF = reviewTable.map(record => record(3).filter(_ > ' ').length).toDF("char_count")

		val avgCharCount = reviewLengthDF.select(round(avg("char_count")).as("average_char_count")).show()

	// Task 2.c: Top 10 business with most reviews


		

	// Task 2.d: Number of reviews per year
	// Task 2.e: Time and date of first and last review
	// Task 2.f: Pearson correlation coefficient 
	}

}