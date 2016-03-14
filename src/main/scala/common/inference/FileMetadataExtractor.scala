package common.inference

import java.util.regex.{Matcher, Pattern}

import common.inference.ValueType.ValueType
import common.parsing.{BooleanParser, DateParser, ParsedDate, TypeParser}

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * Created by markmo on 12/03/2016.
  */
object FileMetadataExtractor {

  val commonDelimiters = List(',', '\t', ';', ' ', '|')

  val numericPattern = Pattern.compile("""^\s*[+-]?(0(?=\.)|[1-9])[0-9]*(\.[0-9]+)?\s*$""")

  val parser = new TypeParser
  parser.register(Boolean.getClass, new BooleanParser)
  parser.register(ParsedDate.getClass, new DateParser)

  def setDateFormats(dateFormats: List[String]): Unit =
    parser.register(ParsedDate.getClass, new DateParser(dateFormats))

  def sniff(data: String, lineEnding: String): FileMetadata = {

    // try regex method
    var metadata = guessQuoteAndDelimiter(data, lineEnding)

    // try statistical method
    if (!metadata.inferred) {
      metadata = guessDelimiter(data, lineEnding)
    }

    metadata
  }

  /**
    * The delimiter /should/ occur the same number of times on each row.
    * However, due to malformed data, it may not. We don't want an all
    * or nothing approach, so we allow for small variations in this number.
    *
    *   1) build a table of the frequency of each character on every line.
    *   2) build a table of frequencies of this frequency (meta-frequency),
    *      e.g. 'x occurred 5 times in 10 rows, 6 times in 1000 rows,
    *      7 times in 2 rows'
    *   3) use the mode of the meta-frequency to determine the /expected/
    *      frequency for that character
    *   4) find out how often the character actually meets that goal
    *   5) the character that best meets its goal is the delimiter
    *      For performance reasons, the data is evaluated in chunks, so it can
    *      try and evaluate the smallest portion of the data possible, evaluating
    *      additional chunks as necessary.
    *
    * @param data String
    * @param lineEnding String
    * @return FileMetadata
    */
  def guessDelimiter(data: String, lineEnding: String): FileMetadata = {
    val rows = data.split(lineEnding)
    val utf8 = for (i <- 0 until 2048) yield i.toChar
    val chunkLength = Math.min(10, rows.length)
    val charFrequency =
      mutable.Map[Char, mutable.Map[Int, Int]]()
        .withDefaultValue(mutable.Map[Int, Int]().withDefaultValue(0))
    val modes = mutable.Map[Char, (Int, Int)]()
    val delims = mutable.Map[Char, (Int, Int)]()

    var start = 0
    var end = Math.min(chunkLength, rows.length)
    var iteration = 0

    while (start < rows.length) {
      iteration += 1
      for (
        line <- rows.toList.slice(start, end);
        char <- utf8
      ) {
        val metaFrequency = charFrequency(char)
        // must count even if frequency is 0
        val freq = line.count(_ == char)
        // value is the mode
        metaFrequency(freq) += 1
        charFrequency(char) = metaFrequency
      }
      for (
        (char, metaFrequency) <- charFrequency
        if !(metaFrequency.size == 1 && metaFrequency(0) == 0)
      ) {
        if (metaFrequency.nonEmpty) {
          val (freqMode, freqCount) = metaFrequency.maxBy(_._2)
          // adjust the mode - subtract the sum of all
          // other frequencies
          val sumOtherFreqCounts = metaFrequency.filter(_._1 != freqMode).values.sum
          modes(char) = (freqMode, freqCount - sumOtherFreqCounts)
        } else {
          modes(char) = metaFrequency.head
        }
      }
      val total = end * iteration

      // (rows of consistent data) / (number of rows) = 100%
      var consistency = 1.0

      // minimum consistency threshold
      val threshold = 0.9

      while (delims.isEmpty && consistency >= threshold) {
        for ((char, (freqMode, adjustedFreqCount)) <- modes) {
          if (freqMode > 0 && adjustedFreqCount > 0) {
            if ((adjustedFreqCount / total) >= consistency) {
              delims(char) = (freqMode, adjustedFreqCount)
            }
          }
        }
        consistency -= 0.01
      }
      if (delims.size == 1) {
        val delim = delims.head._1
        val delimCount = rows(0).count(_ == delim)
        val p = Pattern.compile(delim + " ")
        val m = p.matcher(rows(0))
        var delimWithSpaceCount = 0
        while (m.find(0)) delimWithSpaceCount += 1
        val skipInitialSpace = delimCount == delimWithSpaceCount

        return FileMetadata(delim.toString, skipInitialSpace)
      }
      start = end
      end += chunkLength
      end = Math.min(end, rows.length)
    }

    if (delims.isEmpty) {
      return FileMetadata()
    }

    // if there's more than one, fall back to a 'preferred' list
    for (delim <- commonDelimiters if delims.contains(delim)) {
      val delimCount = rows(0).count(_ == delim)
      val p = Pattern.compile(delim + " ")
      val m = p.matcher(rows(0))
      var delimWithSpaceCount = 0
      while (m.find(0)) delimWithSpaceCount += 1
      val skipInitialSpace = delimCount == delimWithSpaceCount

      return FileMetadata(delim.toString, skipInitialSpace)
    }

    // nothing else indicates a preference, pick the character that dominates
    val (delim, _) = delims.maxBy(_._2)
    val delimCount = rows(0).count(_ == delim)
    val p = Pattern.compile(delim + " ")
    val m = p.matcher(rows(0))
    var delimWithSpaceCount = 0
    while (m.find(0)) delimWithSpaceCount += 1
    val skipInitialSpace = delimCount == delimWithSpaceCount

    FileMetadata(delim.toString, skipInitialSpace)
  }

  /**
    * Looks for text enclosed between two identical quotes (the probable
    * textQualifier) which are preceded and followed by the same character
    * (the probable delimiter).
    * For example:
    *                  ,'some text',
    *
    * The quote with the most wins, same with the delimiter. If there is
    * no textQualifier then the delimiter can't be determined this way.
    *
    * @param data String
    * @param lineEnding String
    * @return FileMetadata
    */
  def guessQuoteAndDelimiter(data: String, lineEnding: String): FileMetadata = {
    val l = lineEnding
    val regexprs = List(
      s"""(?<delim>[^\w$l"']+)(?<space> ?)(?<quote>["']).*?(\k<quote>)(\k<delim>)""",
      s"""(?:^|$l)(?<quote>["']).*?(\k<quote>)(?<delim>[^\w$l"']+)(?<space> ?)""",
      s"""(?<delim>>[^\w$l"']+)(?<space> ?)(?<quote>["']).*?(\k<quote>)(?:$$|$l)""",
      s"""(?:^|$l)(?<quote>["']).*?(\k<quote>)(?:$$|$l)"""
    )

    // embedded construction flag         meaning
    // flag
    // (?i)     Pattern.CASE_INSENSITIVE  Enables case-insensitive matching.
    // (?d)     Pattern.UNIX_LINES        Enables Unix lines mode.
    // (?m)     Pattern.MULTILINE         Enables multi line mode.
    // (?s)     Pattern.DOTALL            Enables "." to match line terminators.
    // (?u)     Pattern.UNICODE_CASE      Enables Unicode-aware case folding.
    // (?x)     Pattern.COMMENTS          Permits white space and comments in the pattern.
    // ---      Pattern.CANON_EQ          Enables canonical equivalence.

    @tailrec
    def loop(rs: List[String]): Option[Matcher] = rs match {
      case Nil => None
      case (re :: rest) =>
        val pat = Pattern.compile(re, Pattern.MULTILINE | Pattern.DOTALL)
        val matches = pat.matcher(data)
        if (matches.find(0)) Some(matches) else loop(rest)
    }

    val maybeMatches = loop(regexprs)

    if (maybeMatches.isDefined) {
      val matches = maybeMatches.get
      val quotes = mutable.Map[String, Int]().withDefaultValue(0)
      val delims = mutable.Map[String, Int]().withDefaultValue(0)
      var spaces = 0
      val quote = matches.group("quote")
      if (quote != null) {
        quotes(quote) += 1
      }
      val delim = matches.group("delim")
      if (delim != null) {
        delims(delim) += 1
        if (matches.group("space") != null) {
          spaces += 1
        }
      }
      val textQualifier = if (quotes.isEmpty) "" else quotes.maxBy(_._2)._1
      val (columnDelimiter, skipInitialSpace) = if (delims.isEmpty) {
        ("", false)
      } else {
        val d = delims.maxBy(_._2)._1
        (
          if (d == lineEnding) {
            // most likely a file with a single column
            ""
          } else {
            d
          },
          delims(d) == spaces
          )
      }
      // if we see an extra quote between delimiters, we've got a
      // double quoted format
      val d = Pattern.quote(columnDelimiter)
      val c = Pattern.quote(columnDelimiter(0).toString)
      val q = Pattern.quote(textQualifier)
      val re = s"""(?m)(($d)|^)\\W*$q[^$c$l]*$q[^$c$l]*$q\W*(($d)|$$)"""
      val p = Pattern.compile(re)
      val m = p.matcher(data)
      val doubleQuoted = m.find(0) && (m.group(1) != null)

      FileMetadata(textQualifier, doubleQuoted, columnDelimiter, skipInitialSpace)

    } else {
      FileMetadata()
    }
  }

  /**
    * Creates a dictionary of types of data in each column. If any
    * column is of a single type (say, integers), *except* for the first
    * row, then the first row is presumed to be labels. If the type
    * can't be determined, it is assumed to be a string in which case
    * the length of the string is the determining factor: if all of the
    * rows except for the first are the same length, it's a header.
    * Finally, a 'vote' is taken at the end for each column, adding or
    * subtracting from the likelihood of the first row being a header.
    *
    * @param data List of sample rows
    * @return Boolean indicates if data probably has header
    */
  def hasHeader(data: List[List[String]]): Boolean = {
    val firstLine = data.head
    val lenColumns = firstLine.size
    val columnTypes = mutable.Map[Int, (ValueType, Int)]()
    for (i <- 0 until lenColumns) {
      columnTypes(i) = (ValueType.None, 0)
    }
    for (
      row <- data if row.length == lenColumns;
      idx <- 0 until lenColumns
    ) {
      val thisType = getType(row(idx))
      if (thisType == columnTypes(idx)) {
        columnTypes(idx) = (ValueType.None, 0)
      } else {
        columnTypes(idx) = thisType
      }
    }
    val hasHeaderVote = columnTypes.foldLeft(0) {
      case (acc, (idx, valueType)) => acc + testHeader(valueType, firstLine(idx))
    }
    hasHeaderVote > 0
  }

  private def testHeader(valueType: (ValueType, Int), value: String): Int = {
    if (value == null) return 0
    val (valType, len) = valueType
    if (valType == ValueType.String) {
      return if (value.length == len) -1 else 1
    }
    if (valType == ValueType.Integer) {
      try {
        val i = Integer.parseInt(value)
        // check that the decimal place is not truncated
        if (i.toString == value) {
          return -1
        }
      } catch {
        case _: NumberFormatException =>
          return 1
      }
    }
    if (valType == ValueType.Numeric) {
      try {
        java.lang.Double.parseDouble(value)
        return -1
      } catch {
        case _: NumberFormatException =>
          return 1
      }
    }
    if (valType == ValueType.Date) {
      val parsedDate = parser.parse(value, ParsedDate.getClass)
      return if (parsedDate.isDefined) -1 else 1
    }
    if (valType == ValueType.Boolean) {
      val bool = parser.parse(value, Boolean.getClass)
      return if (bool.isDefined) -1 else 1
    }
    0
  }

  def getType(str: String): (ValueType, Int) = {
    if (str == null) return (ValueType.None, 0)
    val matcher = numericPattern.matcher(str)
    if (matcher.matches()) {
      try {
        Integer.parseInt(str)
        return (ValueType.Integer, str.length)
      } catch {
        case _: NumberFormatException => // skip
      }
      try {
        java.lang.Double.parseDouble(str)
        return (ValueType.Numeric, str.length)
      } catch {
        case _: NumberFormatException => // skip
      }
    }
    val parsedDate = parser.parse(str, ParsedDate.getClass)
    if (parsedDate.isDefined) return (ValueType.Date, str.length)

    val bool = parser.parse(str, Boolean.getClass)
    if (bool.isDefined) return (ValueType.Boolean, str.length)

    (ValueType.String, str.length)
  }

}
