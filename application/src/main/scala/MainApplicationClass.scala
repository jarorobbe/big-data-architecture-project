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

import java.util.Calendar
import java.util.Date
import java.io.PrintWriter
import java.io.File

object DataProcessor {

	def main (args: Array[String]) {

		val appName = "Big Data Architecture - Project 1"
		val master = "local"

		val pw = new PrintWriter(new File("output.txt"))

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

		pw.write("\n\n----------Task 1----------\n\n")

		val reviewTableRaw = sc.textFile(directory + reviewFileName)
		pw.write("Review Table count: " + reviewTableRaw.count() + "\n")

		val businessTableRaw = sc.textFile(directory + businessFileName)	
		pw.write("Business Table count: " + businessTableRaw.count() + "\n")

		val friendshipGraphRaw = sc.textFile(directory + friendshipFileName)	
		pw.write("Friendship Graph count: " + friendshipGraphRaw.count() + "\n")

// --------------------------------------REVIEW TABLE------------------------------------------ //

	// Task 2.a: Distinct users in dataset

		pw.write("\n\n----------Task 2a---------\n\n")

		// Initially we split each string on each row into a list
		// and remove the header form the dataset
		// We also decode the base64 review_text and remove the '\n' characters
		// TODO: decode unixtimestamp (field 4)
		val header1 = reviewTableRaw.first()
		val reviewTable = reviewTableRaw.filter(line => line != header1).map(line => line.split("\t"))
				.map(record => List(record(0),record(1),record(2),
					(new String(java.util.Base64.getMimeDecoder().decode(record(3)))).filter(_ >= ' ')
					,record(4)))



		// Map the RDD on a new RDD and only keep the user_id column, the distinct function only takes the unique rows
		val reviewTableOnlyUsersDistinct = reviewTable.map(record => record(1)).distinct()
		pw.write("Distinct users: " + reviewTableOnlyUsersDistinct.count() + "\n")

	// Task 2.b: Average number of characters in user review


		pw.write("\n\n----------Task 2b---------\n\n")
		

		// First, we only keep the review_text in our dataset
		// Then we filter out all the whitespaces and save how many records there are in the dataset (necessary for calculating the avergare character count)
		// Finaly, we reduce the dataset by adding all the values and keeping count of the number of records at the same time
		// This results in a tuple with the summation of all the number of characters and the number of records, 
		// we can calcualte the average character count by dividing these numbers
		// The following condition "_ > ' '" filters out all characters larger than space, it is used to remove all whitespace characters like \n and \t

		val count = reviewTable.map(record => (record(3).filter(_ > ' ').length.toInt, 1)).reduce((x, acc) => (x._1+acc._1, x._2+acc._2))
		val average = count._1/count._2

		pw.write("Average character count (without whitespaces): " + average + "\n")

		// Alternative with DataFrame (a little bit slower):
		// val reviewLengthDF = reviewTable.map(record => record(3).filter(_ > ' ').length).toDF("char_count")
		// val avgCharCount = reviewLengthDF.select(round(avg("char_count")).as("average_char_count")).show()

	// Task 2.c: Top 10 business with most reviews

		pw.write("\n\n----------Task 2c---------\n\n")

		// First, we create key value pairs: (business_id, 1)
		// Next we reduce the dataset and add all the 1's with matching business_id
		// Finaly we sort this dataset by the 2nd value of the pair and take the first 10 items
		val reviewTableByBusinnesId = reviewTable.map(record => (record(2), 1)).reduceByKey((a,b) => a + b).sortBy(tpl => tpl._2, false).take(10)
		pw.write("Top 10 business with most reviews:" + "\n")
		reviewTableByBusinnesId.foreach(x => pw.write(x.toString + "\n"))

	// Task 2.d: Number of reviews per year

		pw.write("\n\n----------Task 2d---------\n\n")
		
		// First, we create key value pairs: (Year, 1)
		// Next, we reduce the dataset by the year and add the 2nd value of the corresponding pairs
		// This results in a count of the reviews for each year
		val calendar = Calendar.getInstance()
		val reviewTableByYear = reviewTable.map(record => {
				calendar.setTimeInMillis((record(4).toDouble*1000).toLong)
				val year = calendar.get(Calendar.YEAR)
				(year, 1)
			}).reduceByKey((a,b) => a + b).sortBy(tpl => tpl._1)

		pw.write("Number of reviews per year: " + "\n")
		reviewTableByYear.collect().foreach(x => pw.write(x.toString + "\n"))

	// Task 2.e: Time and date of first and last review

		pw.write("\n\n----------Task 2e---------\n\n")

		// We can use the min and max function of the RDD in order to calculate the eldest and latest review
		val min = (reviewTable.map(record => record(4).toDouble).min()*1000).toLong
		calendar.setTimeInMillis(min)
		pw.write("Eldest review: " + calendar.getTime() + "\n")
		val max = (reviewTable.map(record => record(4).toDouble).max()*1000).toLong
		calendar.setTimeInMillis(max)
		pw.write("Latest review: " + calendar.getTime() + "\n")

	// Task 2.f: Pearson correlation coefficient 

		pw.write("\n\n----------Task 2f---------\n\n")

		// X = number of reviews by a user
		// Y = average number of characters in the reviews of a user

		val X = reviewTable.map(record => (record(1), 1)).reduceByKey((a,b) => a + b)
		val Y = reviewTable.map(record => (record(1), (record(3).filter(_ > ' ').length.toInt, 1)))
			.reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
			.map(tpl => (tpl._1, tpl._2._1/tpl._2._2))

		val XavgPair = X.map(tpl => (tpl._2, 1)).reduce((a,b) => (a._1+b._1, a._2+b._2))
		val Xavg = XavgPair._1/XavgPair._2
		val YavgPair = Y.map(tpl => (tpl._2, 1)).reduce((a,b) => (a._1+b._1, a._2+b._2))
		val Yavg = YavgPair._1/YavgPair._2	

		val XandY = X.join(Y).map(tpl => (tpl._1, ((tpl._2._1, tpl._2._1 - Xavg), (tpl._2._2, tpl._2._2 - Yavg))))

		val part1 = XandY.map(tpl => (tpl._2._1._2 * tpl._2._2._2)).reduce((a,b) => a + b)
		val part2 = X.map(tpl => (scala.math.pow((tpl._2 - Xavg), 2))).reduce((a,b) => a + b)
		val part3 = Y.map(tpl => (scala.math.pow((tpl._2 - Yavg), 2))).reduce((a,b) => a + b)

		pw.write("Pearson correlation: " + (part1/(scala.math.sqrt(part2)*scala.math.sqrt(part3))) + "\n")


// --------------------------------------BUSINESS TABLE---------------------------------------- //
	
	// Task 3.a: Average business rating in each city

		pw.write("\n\n----------Task 3a---------\n\n")

		// First we clean the raw data from the business table (the same way as we cleaned the reviews table)
		val header2 = businessTableRaw.first()
		val businessTable = businessTableRaw.filter(line => line != header2).map(line => line.split("\t").toList)

		// We take the city and the rating and keep for every rating a counter inside a tuple
		// We then reduce the RDD by key (cities), for each city we add the ratings and count how many ratings there are
		// Finaly, we divide for every city the sum of the ratings by the number of ratings in that city
		val ratingSum = businessTable.map(record => (record(3), (record(8).toString.toFloat, 1)))
			.reduceByKey((a,b) => (a._1+b._1, a._2+b._2))
			.map(tpl => (tpl._1, tpl._2._1/tpl._2._2))

		pw.write("First 10 cities with their average business rating:" + "\n")
		ratingSum.take(10).foreach(x => pw.write(x.toString + "\n"))


	// Task 3.b: Top 10 most frequent categories in the data

		pw.write("\n\n----------Task 3b---------\n\n")

		// We start by splitting the category field and removing unnecessary whitespaces
		// Next, we explode the array of categories (all the elements of the arrays in the category column become separate rows)
		// Finlay, we can map a counter to each category, reduce the categories and count their occurances and sort them by number of occurances
		val categoryFrequency = businessTable.map(record => record(10).toString.filter(x => ((x > ' ') && (x != '"'))).split(","))
			.flatMap(array => array)
			.map(category => (category, 1))
			.reduceByKey((a,b) => a + b)
			.sortBy(tpl => tpl._2, false)
			
		pw.write("Top 10 most frequent categories:" + "\n")
		categoryFrequency.take(10).foreach(x => pw.write(x.toString + "\n"))


	// Task 3.c: Geopgraphical centroid of region (postal code)

		pw.write("\n\n----------Task 3c---------\n\n")

		// We start by cleaning the postal codes (no '"' characters) and adding a counter to the latitude and longitude 
		// Next, we calculate the sum of the lat and long, and the number of rows for each postal code
		// We then use these values to calculate the mean lat and long for every postal code
		val geopgraphicalCentroid = businessTable.map(record => (record(5).filter(_ != '"'), ((record(6).toString.toFloat, 1), (record(7).toString.toFloat, 1))))
			.reduceByKey((a,b) => ((a._1._1+b._1._1, a._1._2+b._1._2),(a._2._1+b._2._1,a._2._2+b._2._2)))
			.map(tpl => (tpl._1, (tpl._2._1._1/tpl._2._1._2, tpl._2._2._1/tpl._2._2._2)))

		pw.write("First 10 regions with their geopgraphical centroid:" + "\n")
		geopgraphicalCentroid.take(10).foreach(x => pw.write(x.toString + "\n"))


// -------------------------------------FRIENDSHIP GRAPH--------------------------------------- //
		sc.stop()
		pw.close()
	}

}