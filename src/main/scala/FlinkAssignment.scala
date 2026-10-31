import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.windowing.time.Time
import util.Protocol.{Commit, CommitGeo, CommitSummary, File, Stats}
import util.{CommitGeoParser, CommitParser}



/** Do NOT rename this class, otherwise autograding will fail. **/
object FlinkAssignment {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    /**
      * Setups the streaming environment including loading and parsing of the datasets.
      *
      * DO NOT TOUCH!
      */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Read and parses commit stream.
    val commitStream =
      env
        .readTextFile("data/flink_commits.json")
        .map(new CommitParser)

    // Read and parses commit geo stream.
    val commitGeoStream =
      env
        .readTextFile("data/flink_commits_geo.json")
        .map(new CommitGeoParser)

    /** Use the space below to print and test your questions. */
    // question_nine(commitStream).print()
    // question_two(commitStream).print()

    /** Start the streaming environment. **/
    env.execute()
  }

  /** Dummy question which maps each commits to its SHA. */
  def dummy_question(input: DataStream[Commit]): DataStream[String] = {
    input.map(_.sha)
  }

  /**
    * Write a Flink application which outputs the sha of commits with at least 20 additions.
    * Output format: sha
    */
  def question_one(input: DataStream[Commit]): DataStream[String] = {
    input.filter(c => c.stats.getOrElse(Stats(0,0,0)).additions >= 20).map(_.sha)
  }

  /**
    * Write a Flink application which outputs the names of the files with more than 30 deletions.
    * Output format:  fileName
    */
  def question_two(input: DataStream[Commit]): DataStream[String] = {
    input
      .flatMap(_.files) // from commits to files
      .filter(_.deletions > 30) // keep files with more than 30 deletions
      .flatMap(_.filename)  // extract filenames and discard "none"
  }

  /**
    * Count the occurrences of Java and Scala files. I.e. files ending with either .scala or .java.
    * Output format: (fileExtension, #occurrences)
    */
  def question_three(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input.flatMap(i => i.files).map(new ThreeOne).filter(i => i.isDefined).map(new ThreeTwo).filter(i => i._1.equals(".scala") ||i._1.equals(".java")).keyBy(i => i._1).reduce((a,b) => (a._1, a._2 +b._2))

  }

  /**
    * Count the total amount of changes for each file status (e.g. modified, removed or added) for the following extensions: .js and .py.
    * Output format: (extension, status, count)
    */
  def question_four(input: DataStream[Commit]): DataStream[(String, String, Int)] = {
    input
      .flatMap(_.files) // from commits to files
      .filter(f => f.filename.isDefined && f.status.isDefined) // discard "none"
      .map(f => (f.filename.get.split('.').last, f.status.get, f.changes)) // (ext, status, changes)
      .filter(t => t._1 == "js" || t._1 == "py") // only keep js and py extensions
      .keyBy(t => (t._1, t._2)) // key = (ext, status)
      .reduce((a, b) => (a._1, a._2, a._3 + b._3)) // running sum of changes per key

  }

  /**
    * For every day output the amount of commits. Include the timestamp in the following format dd-MM-yyyy; e.g. (26-06-2019, 4) meaning on the 26th of June 2019 there were 4 commits.
    * Make use of a non-keyed window.
    * Output format: (date, count)
    */
  def question_five(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input.assignAscendingTimestamps(i => i.commit.committer.date.getTime).map(new FiveOne).timeWindowAll(Time.days(1)).reduce((a, b) => (a._1, a._2+b._2))
  }

  /**
    * Consider two types of commits; small commits and large commits whereas small: 0 <= x <= 20 and large: x > 20 where x = total amount of changes.
    * Compute every 12 hours the amount of small and large commits in the last 48 hours.
    * Output format: (type, count)
    */
  def question_six(input: DataStream[Commit]): DataStream[(String, Int)] = ???

  /**
    * For each repository compute a daily commit summary and output the summaries with more than 20 commits and at most 2 unique committers. The CommitSummary case class is already defined.
    *
    * The fields of this case class:
    *
    * repo: name of the repo.
    * date: use the start of the window in format "dd-MM-yyyy".
    * amountOfCommits: the number of commits on that day for that repository.
    * amountOfCommitters: the amount of unique committers contributing to the repository.
    * totalChanges: the sum of total changes in all commits.
    * topCommitter: the top committer of that day i.e. with the most commits. Note: if there are multiple top committers; create a comma separated string sorted alphabetically e.g. `georgios,jeroen,wouter`
    *
    * Hint: Write your own ProcessWindowFunction.
    * Output format: CommitSummary
    */
  def question_seven(
      commitStream: DataStream[Commit]): DataStream[CommitSummary] = {
    commitStream.assignAscendingTimestamps(i => i.commit.committer.date.getTime).keyBy(i  => i.url.split('/')(4)+i.url.split('/')(5)).timeWindow(Time.days(1)).aggregate(new SevenTwo).filter(i => i.amountOfCommits>20 && i.amountOfCommitters <=2)

  }

  /**
    * For this exercise there is another dataset containing CommitGeo events. A CommitGeo event stores the sha of a commit, a date and the continent it was produced in.
    * You can assume that for every commit there is a CommitGeo event arriving within a timeframe of 1 hour before and 30 minutes after the commit.
    * Get the weekly amount of changes for the java files (.java extension) per continent. If no java files are changed in a week, no output should be shown that week.
    *
    * Hint: Find the correct join to use!
    * Output format: (continent, amount)
    */
  def question_eight(
      commitStream: DataStream[Commit],
      geoStream: DataStream[CommitGeo]): DataStream[(String, Int)] = ???

  /**
    * Find all files that were added and removed within one day. Output as (repository, filename).
    *
    * Hint: Use the Complex Event Processing library (CEP).
    * Output format: (repository, filename)
    */
  def question_nine(
      inputStream: DataStream[Commit]): DataStream[(String, String)] = {
    val pattern = Pattern.begin("start").where((i :(String, File)) =>  i._2.status.getOrElse("").equals("added")).next("removed").where((i :(String, File)) =>  i._2.status.getOrElse("").equals("removed")).within(Time.days(1))

    val changed = inputStream.assignAscendingTimestamps((i : Commit) => i.commit.committer.date.getTime).flatMap[(String, File)]((t : Commit) => t.files.map(i => (t.url.split('/')(4)+"/"+t.url.split('/')(5), i))).filter(k => !k._2.status.getOrElse("modified").equals("modified")).keyBy((i : (String, File)) => i._1+i._2.filename.getOrElse(""))


    CEP.pattern(changed, pattern).process(new NineOne)
  }

}
