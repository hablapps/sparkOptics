package org.hablapps.sparkOptics

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.scalatest.{Matchers, Suite}

trait LensLaws extends DataFrameSuiteBase with Matchers {
  self: Suite =>
  def getSet(l: Lens, s: DataFrame): Unit = {
    assertDataFrameEquals(s, s.select(l.set(l.get): _*))
  }

  def setGet[A](l: Lens, s: DataFrame, a: A): Unit = {
    import s.sparkSession.implicits._
    s.select(l.set(lit(a)): _*)
      .groupBy(l.get)
      .count()
      .as[(String, Long)]
      .head() shouldBe(a.toString, s.count())
  }
}
