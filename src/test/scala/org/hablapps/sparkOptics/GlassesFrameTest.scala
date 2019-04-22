package org.hablapps.sparkOptics

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpecLike, Matchers}
import org.apache.spark.sql.functions.{concat, length => columnLength, lit}

class GlassesFrameTest extends FlatSpecLike with Matchers with DataFrameSuiteBase {
  import spark.implicits._
  import org.hablapps.sparkOptics.syntax._


  "Lens glasses frame" should "keep the schema in modification" in {
    val l: ProtoLens = Lens("a")
    val df = List("hola", "adios").toDF("a")
    noException should be thrownBy  l.modifyDFCheckingSchema(x => concat(x,x))(df)
    the [TypeChangedException[Lens]] thrownBy {
      l.modifyDFCheckingSchema(x => columnLength(x))(df)
    } should have message s"The original column type that was StringType changed to IntegerType using $l"
  }

  it should "keep the schema in set" in {
    val l: ProtoLens = Lens("a")
    val df = List("hola", "adios").toDF("a")
    the [NullableChangedException[Lens]] thrownBy {
      l.setDFCheckingSchema(lit("aaa"))(df)
    } should have message s"The original column nullable that was true changed to false using $l"
    the [TypeChangedException[Lens]] thrownBy {
      l.setDFCheckingSchema(lit(23))(df)
    } should have message s"The original column type that was StringType changed to IntegerType using $l"
  }
}
