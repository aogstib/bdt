//package com.nmit.spark.wordcount

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

/**
  * Problem statement:
  * Here the goal is to count how many times each word appears in a file and write out a list of
  * words whose count is strictly greater than 4. 
  * Use the file log.txt accompanying this assignment to count the words.
 */

object wordcount {

  def main(args: Array[String]) {

    val pathToFile = "log.txt"

    // create spark configuration and spark context: the Spark context is the entry point in Spark.
    // It represents the connexion to Spark and it is the place where you can configure
    // the common properties
    // like the app name, the master url, memories allocation...

    val conf = new SparkConf().setAppName("Wordcount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    // load data and create an RDD where each element will be a word
    // Here the flatMap method is used to separate the word in each line using the space separator
    // You can experiment with "map" instead of "flatMap"to understand why flatMap is required.

    val wordsRdd = sc.textFile(pathToFile).flatMap(_.split(" "))

    /**
      *  Now count how many times each word appears!
      */

    // Step 1: the mapper step
    // We want to attribute the number 1 to each word: so we create couples (word, 1).

    val wordCountInitRdd = wordsRdd.map(word => (word, 1))

    // Step 2: reducer step
    // Now you have a tuple (key, 1) where the key is a word,
    // you want to count the occurrences of (key, 1). 
    // One way to do this is by using the reduce operation. 

    val wordCountRdd = wordCountInitRdd.reduceByKey((v1, v2) => v1 + v2)
    // wordCountRdd.take(10).foreach(println)

    // Step 3: Filter Step
    // Now keep those words which appear strictly more than 4 times!
    // You can do this using the filter operation.

    val highFreqWords = wordCountRdd.filter(x => x._2 > 4)

    // save the word counts in a textfile "wordcountsDir".

    highFreqWords.saveAsTextFile("wordcountsDir")
  }
}
