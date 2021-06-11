# scala
Various Scala and Spark Projects

Project was to total the combined purchases of each customer.  Then sort by those totals.  I added a challenge of finding those with expenditures greater than $5,000

```Scala

package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._
import org.sparkproject.dmg.pmml.False
import scala.math.max

object BigSpender {

  def parseLine(line: String): (Int, Float) = {
        // Split by commas
    val fields = line.split(",")
        // Extract the customerID and amount spent in that transaction
    val CustID = fields(0).toInt
    val AmountSpent = fields(2).toFloat
        // Create a tuple that is our result.
    (CustID, AmountSpent)
  }


  def main(args: Array[String]) {

///////////////////////////////////////////////////////////////////////////////

   //  Pre-processing 

///////////////////////////////////////////////////////////////////////////////

        // Formatter for displaying currency results
    val formatter = java.text.NumberFormat.getCurrencyInstance
    
        // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

        // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "BigSpenderTBrown")

        // Load each transaction into an RDD
    val lines = sc.textFile("data/customer-orders.csv")

        //  extract the customerID and amount spent in each transaction
    val transactionList = lines.map(parseLine)

        // use reduce function to total up all the spending per customerID
    val CustomerTotals = transactionList.reduceByKey((x,y) => x + y )

        // Swap columns to make the totals spent the key values to then sort by key
    val sortedTotals = CustomerTotals.map(x => (x._2, x._1)).sortByKey()

        // collect the sorted results
    val results = sortedTotals.collect()

///////////////////////////////////////////////////////////////////////////////

    // print the total spending for every customer in order of amount spent
        
///////////////////////////////////////////////////////////////////////////////        

    for (result <- results) {
      val total = result._1
      val custID = result._2
      val formattedTotal = formatter.format(total)
      println(s"Customer $custID spent a total amount of $formattedTotal")
    }

///////////////////////////////////////////////////////////////////////////////

       // Find the big spender
       
///////////////////////////////////////////////////////////////////////////////

    var maxTotal: Float = 0f
    var ID : Int = 0

    val rawResults = CustomerTotals.collect()

    for (person <- rawResults) {
       if (maxTotal <= person._2) {
        maxTotal = person._2
        ID = person._1
      }
    }
    println(f"Customer $ID spent the most with a total amount of $maxTotal%.2f")

///////////////////////////////////////////////////////////////////////////////

      // Find customers who spent over certain amount 
      
///////////////////////////////////////////////////////////////////////////////      

    val limit = 6000
    val TopCustomers = CustomerTotals.filter(x => x._2 > limit).collect()
    
    for (person <- TopCustomers) {
      val formattedLimit = formatter.format(limit)
      val formattedTotal = formatter.format(person._2)
      val CustID = person._1
      println(f"Customer $CustID spent over $formattedLimit with a total amount of $formattedTotal")
    }
  }
}
