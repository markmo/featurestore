package common.parsing

import org.joda.time.format.DateTimeFormat

import scala.annotation.tailrec

/**
  * Created by markmo on 12/03/2016.
  */
class DateParser(var formats: List[String]) extends Parser[ParsedDate] {

  def this() = this(List(
    // pattern                      // example
    "yyyy-MM-dd'T'HH:mm:ss.SSSZ",   // 2001-07-04T12:08:56.235-0700
    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", // 2001-07-04T12:08:56.235-07:00
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS", // 2001-07-04T12:08:56.235000
    "yyyy-MM-dd HH:mm:ss.SSSZ",     // 2001-07-04 12:08:56.235-0700
    "yyyy-MM-dd HH:mm:ss.SSSXXX",   // 2001-07-04 12:08:56.235-07:00
    "yyyy-MM-dd HH:mm:ss.SSSSSS",   // 2001-07-04 12:08:56.235000
    "yyyyMMdd HH:mm:ss",            // 20010704 12:08:56
    "EEE, MMM d, ''yy",             // Wed, Jul 4, '01
    "EEE, MMM d, yyyy",             // Wed, Jul 4, 2001
    "yyyy.MM.dd",                   // 2001.07.04
    "yyyy-MM-dd",                   // 2001-07-04
    "yyyy/MM/dd",                   // 2001/07/04
    "dd.MM.yyyy",                   // 04.07.2001
    "dd-MM-yyyy",                   // 04-07-2001
    "dd/MM/yyyy",                   // 04/07/2001
    "MM.dd.yyyy",                   // 07.04.2001
    "MM-dd-yyyy",                   // 07-04-2001
    "MM/dd/yyyy",                   // 07/04/2001
    "dd.MM.yy",                     // 04.07.01
    "dd-MM-yy",                     // 04-07-01
    "dd/MM/yy",                     // 04/07/01
    "MM.dd.yy",                     // 07.04.01
    "MM-dd-yy",                     // 07-04-01
    "MM/dd/yy",                     // 07/04/01
    "dd/MMM/yy",                    // 03/APR/15
    "yyyy-MM-dd",
    "yyyy-MM-dd'T'HH",
    "yyyy-MM-dd HH",
    "yyyy-MM-dd'T'HH:mm",
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss Z",
    "yyyy-MM-dd HH:mm:ss Z"
  ))

  def parse(value: Any): Option[ParsedDate] =
    if (value == null) {
      None
    } else {
      val x = value.toString.trim
      if (x.isEmpty) {
        None
      } else {

        @tailrec
        def loop(fmts: List[String]): Option[ParsedDate] = fmts match {
          case Nil => None
          case (fmt :: rest) =>
            try {
              val formatter = DateTimeFormat.forPattern(fmt)
              val dt = formatter.parseDateTime(x)

              // move format to top of list
              formats = moveToTop(fmt, formats)

              Some(ParsedDate(dt, fmt))
            } catch {
              case e: IllegalArgumentException => loop(rest)
            }
        }
        loop(formats)
      }
    }

  def moveToTop(y: String, xs: List[String]) =
    xs.span(_ != y) match {
      case (as, h :: bs) => h :: as ++ bs
      case _ => xs
    }

}
