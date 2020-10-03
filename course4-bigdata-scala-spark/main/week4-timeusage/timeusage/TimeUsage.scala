package timeusage

import org.apache.spark.sql._
import org.apache.spark.sql.types._

/** Main class */
object TimeUsage extends TimeUsageInterface {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .master("local")
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames

  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {
    timeUsageByLifePeriod()
    spark.close()
  }

  def timeUsageByLifePeriod(): Unit = {
    val (columns, initDf) = read("src/main/resources/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    //    val finalDf = timeUsageGrouped(summaryDf)
    //    val finalDf = timeUsageGroupedSql(summaryDf)
    val finalDf = timeUsageGroupedTyped(timeUsageSummaryTyped(summaryDf))
    finalDf.show()
  }

  /** @return The read DataFrame along with its column names. */
  def read(path: String): (List[String], DataFrame) = {
    val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(path)
    (df.schema.fields.map(_.name).toList, df)
  }

  /** @return An RDD Row compatible with the schema produced by `dfSchema`
   * @param line Raw fields
   */
  def row(line: List[String]): Row = {
    val columnInDouble = line.tail.map(_.toDouble)
    Row.merge(Row(line.head), Row.fromSeq(columnInDouble))
  }

  /** @return The initial data frame columns partitioned in three groups: primary needs (sleeping, eating, etc.),
   *          work and other (leisure activities)
   * @see https://www.kaggle.com/bls/american-time-use-survey
   *
   *      The dataset contains the daily time (in minutes) people spent in various activities. For instance, the column
   *      “t010101” contains the time spent sleeping, the column “t110101” contains the time spent eating and drinking, etc.
   *
   *      This method groups related columns together:
   * 1. “primary needs” activities (sleeping, eating, etc.). These are the columns starting with “t01”, “t03”, “t11”,
   *      “t1801” and “t1803”.
   * 2. working activities. These are the columns starting with “t05” and “t1805”.
   * 3. other activities (leisure). These are the columns starting with “t02”, “t04”, “t06”, “t07”, “t08”, “t09”,
   *      “t10”, “t12”, “t13”, “t14”, “t15”, “t16” and “t18” (those which are not part of the previous groups only).
   */
  def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column]) = {
    type Classified = (List[Column], List[Column], List[Column])

    @scala.annotation.tailrec
    def loop(columns: List[String], primary: List[Column], working: List[Column], others: List[Column]): Classified =
      columns match {
        case Nil => (primary, working, others)
        case x :: xs if x.startsWith("t01") || x.startsWith("t03") || x.startsWith("t11") ||
          x.startsWith("t1801") || x.startsWith("1803") => loop(xs, col(x) :: primary, working, others)
        case x :: xs if x.startsWith("t05") || x.startsWith("t1805") =>
          loop(xs, primary, col(x) :: working, others)
        case x :: xs if x.startsWith("t02") || x.startsWith("t04") || x.startsWith("t06") ||
          x.startsWith("t07") || x.startsWith("t08") || x.startsWith("t09") ||
          x.startsWith("t10") || x.startsWith("t12") || x.startsWith("t13") ||
          x.startsWith("t14") || x.startsWith("t15") || x.startsWith("t16") ||
          x.startsWith("t18") => loop(xs, primary, working, col(x) :: others)
        case _ :: xs => loop(xs, primary, working, others)
      }

    loop(columnNames, List(), List(), List())
  }

  /** @return a projection of the initial DataFrame such that all columns containing hours spent on primary needs
   *          are summed together in a single column (and same for work and leisure). The “teage” column is also
   *          projected to three values: "young", "active", "elder".
   * @param primaryNeedsColumns List of columns containing time spent on “primary needs”
   * @param workColumns         List of columns containing time spent working
   * @param otherColumns        List of columns containing time spent doing other activities
   * @param df                  DataFrame whose schema matches the given column lists
   *
   *                            This methods builds an intermediate DataFrame that sums up all the columns of each group of activity into
   *                            a single column.
   *
   *                            The resulting DataFrame should have the following columns:
   * - working: value computed from the “telfs” column of the given DataFrame:
   *   - "working" if 1 <= telfs < 3
   *   - "not working" otherwise
   * - sex: value computed from the “tesex” column of the given DataFrame:
   *   - "male" if tesex = 1, "female" otherwise
   * - age: value computed from the “teage” column of the given DataFrame:
   *   - "young" if 15 <= teage <= 22,
   *   - "active" if 23 <= teage <= 55,
   *   - "elder" otherwise
   * - primaryNeeds: sum of all the `primaryNeedsColumns`, in hours
   * - work: sum of all the `workColumns`, in hours
   * - other: sum of all the `otherColumns`, in hours
   *
   *                            Finally, the resulting DataFrame should exclude people that are not employable (ie telfs = 5).
   *
   *                            Note that the initial DataFrame contains time in ''minutes''. You have to convert it into ''hours''.
   */
  def timeUsageSummary(
                        primaryNeedsColumns: List[Column],
                        workColumns: List[Column],
                        otherColumns: List[Column],
                        df: DataFrame
                      ): DataFrame = {
    // Transform the data from the initial dataset into data that make
    // more sense for our use case
    // Hint: you can use the `when` and `otherwise` Spark functions
    // Hint: don’t forget to give your columns the expected name with the `as` method
    val workingStatusProjection: Column = when(df("telfs") >= 1 && df("telfs") < 3, "working")
      .otherwise("not working")
      .as("working")
    val sexProjection: Column = when(df("tesex") === 1, "male")
      .otherwise("female")
      .as("sex")
    val ageProjection: Column = when(df("teage") >= 15 && df("teage") <= 22, "young")
      .when(df("teage") >= 23 && df("teage") <= 55, "active")
      .otherwise("elder")
      .as("age")

    // Create columns that sum columns of the initial dataset
    // Hint: you want to create a complex column expression that sums other columns
    //       by using the `+` operator between them
    // Hint: don’t forget to convert the value to hours
    val primaryNeedsProjection: Column = toHours(primaryNeedsColumns).as("primaryNeeds")
    val workProjection: Column = toHours(workColumns).as("work")
    val otherProjection: Column = toHours(otherColumns).as("other")
    df
      .select(workingStatusProjection, sexProjection, ageProjection, primaryNeedsProjection, workProjection, otherProjection)
      .where($"telfs" <= 4) // Discard people who are not in labor force
  }

  // Convert minutes in column to hours
  private def toHours(minutes: List[Column]): Column = minutes.reduce(_ + _) / 60.0

  /** @return the average daily time (in hours) spent in primary needs, working or leisure, grouped by the different
   *          ages of life (young, active or elder), sex and working status.
   * @param summed DataFrame returned by `timeUsageSumByClass`
   *
   *               The resulting DataFrame should have the following columns:
   * - working: the “working” column of the `summed` DataFrame,
   * - sex: the “sex” column of the `summed` DataFrame,
   * - age: the “age” column of the `summed` DataFrame,
   * - primaryNeeds: the average value of the “primaryNeeds” columns of all the people that have the same working
   *               status, sex and age, rounded with a scale of 1 (using the `round` function),
   * - work: the average value of the “work” columns of all the people that have the same working status, sex
   *               and age, rounded with a scale of 1 (using the `round` function),
   * - other: the average value of the “other” columns all the people that have the same working status, sex and
   *               age, rounded with a scale of 1 (using the `round` function).
   *
   *               Finally, the resulting DataFrame should be sorted by working status, sex and age.
   */
  def timeUsageGrouped(summed: DataFrame): DataFrame = {
    summed.groupBy($"working", $"sex", $"age")
      .agg(round(avg($"primaryNeeds"), 1).as("primaryNeeds"),
        round(avg($"work"), 1).as("work"),
        round(avg($"other"), 1).as("other"))
      .sort($"working", $"sex", $"age")
  }

  /**
   * @return Same as `timeUsageGrouped`, but using a plain SQL query instead
   * @param summed DataFrame returned by `timeUsageSumByClass`
   */
  def timeUsageGroupedSql(summed: DataFrame): DataFrame = {
    val viewName = s"summed"
    summed.createOrReplaceTempView(viewName)
    spark.sql(timeUsageGroupedSqlQuery(viewName))
  }

  /** @return SQL query equivalent to the transformation implemented in `timeUsageGrouped`
   * @param viewName Name of the SQL view to use
   */
  def timeUsageGroupedSqlQuery(viewName: String): String =
    "SELECT working, sex, age, " +
      "ROUND(AVG(primaryNeeds), 1) AS primaryNeeds, " +
      "ROUND(AVG(work), 1) AS other, " +
      "ROUND(AVG(other), 1) AS other " +
      s"FROM $viewName " +
      "GROUP BY working, sex, age " +
      "ORDER BY working, sex, age"

  /**
   * @return A `Dataset[TimeUsageRow]` from the “untyped” `DataFrame`
   * @param timeUsageSummaryDf `DataFrame` returned by the `timeUsageSummary` method
   *
   *                           Hint: you should use the `getAs` method of `Row` to look up columns and
   *                           cast them at the same time.
   */
  def timeUsageSummaryTyped(timeUsageSummaryDf: DataFrame): Dataset[TimeUsageRow] =
  // OR EASILY AS : timeUsageSummaryDf.as[TimeUsageRow]
    timeUsageSummaryDf.map(df => TimeUsageRow(
      working = df.getAs[String]("working"),
      sex = df.getAs[String]("sex"),
      age = df.getAs[String]("age"),
      primaryNeeds = df.getAs[Double]("primaryNeeds"),
      work = df.getAs[Double]("work"),
      other = df.getAs[Double]("other")
    ))

  /**
   * @return Same as `timeUsageGrouped`, but using the typed API when possible
   * @param summed Dataset returned by the `timeUsageSummaryTyped` method
   *
   *               Note that, though they have the same type (`Dataset[TimeUsageRow]`), the input
   *               dataset contains one element per respondent, whereas the resulting dataset
   *               contains one element per group (whose time spent on each activity kind has
   *               been aggregated).
   *
   *               Hint: you should use the `groupByKey` and `typed.avg` methods.
   */
  def timeUsageGroupedTyped(summed: Dataset[TimeUsageRow]): Dataset[TimeUsageRow] = {
    import org.apache.spark.sql.expressions.scalalang.typed
    summed.groupByKey(s => (s.working, s.sex, s.age))
      .agg(round(avg($"primaryNeeds"), 1).as[Double],
        round(avg($"work"), 1).as[Double],
        round(avg($"other"), 1).as[Double])
      .map { case (keys, primary, work, other) => TimeUsageRow(keys._1, keys._2, keys._3, primary, work, other) }
      .orderBy($"working", $"sex", $"age")
  }
}

/**
 * Models a row of the summarized data set
 *
 * @param working      Working status (either "working" or "not working")
 * @param sex          Sex (either "male" or "female")
 * @param age          Age (either "young", "active" or "elder")
 * @param primaryNeeds Number of daily hours spent on primary needs
 * @param work         Number of daily hours spent on work
 * @param other        Number of daily hours spent on other activities
 */
case class TimeUsageRow(
                         working: String,
                         sex: String,
                         age: String,
                         primaryNeeds: Double,
                         work: Double,
                         other: Double
                       )


// Printed Result (for all cases)
/**
 * +-----------+------+------+------------+----+-----+
 * |    working|   sex|   age|primaryNeeds|work|other|
 * +-----------+------+------+------------+----+-----+
 * |not working|female|active|        12.3| 0.5| 11.0|
 * |not working|female| elder|        10.9| 0.4| 12.4|
 * |not working|female| young|        12.4| 0.2| 11.2|
 * |not working|  male|active|        11.3| 0.9| 11.5|
 * |not working|  male| elder|        10.7| 0.7| 12.4|
 * |not working|  male| young|        11.6| 0.2| 11.9|
 * |    working|female|active|        11.4| 4.2|  8.3|
 * |    working|female| elder|        10.6| 3.9|  9.3|
 * |    working|female| young|        11.6| 3.3|  8.9|
 * |    working|  male|active|        10.8| 5.2|  7.9|
 * |    working|  male| elder|        10.4| 4.8|  8.7|
 * |    working|  male| young|        10.9| 3.7|  9.2|
 * +-----------+------+------+------------+----+-----+
 */