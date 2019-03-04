package org.hablapps.sparkOptics

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpecLike, Matchers}

class LensTest
  extends FlatSpecLike
    with LensLaws
    with Matchers
    with SharedSparkContext {

  import spark.implicits._

  "Spark Lens" should "modify a first level column" in {
    val df = List(
      ("str", 54, "str2"),
      ("str", 54, "str2"),
      ("str", 54, "str2")
    ).toDF("colstr1", "colnum", "colstr2")

    val strLens: Lens = Lens("colstr1")(df.schema)
    val numLens: Lens = Lens("colnum")(df.schema)

    getSet(strLens, df)
    setGet(strLens, df, "aaa")
    getSet(numLens, df)
    setGet(numLens, df, 12)

  }

  it should "modify a n level column" in {
    val increaseLevel: DataFrame => DataFrame = df => {
      val newColumn = struct(df.schema.fields.map(f => col(f.name)): _*)
      df.withColumn("level", newColumn)
    }

    def increaseNLevels(n: Int): DataFrame => DataFrame = {
      (1 to n).map(_ => increaseLevel).reduce(_ andThen _)
    }

    val leveleddf = {
      val df = List(("str", 54, "str2"),
        ("str", 54, "str2"),
        ("str", 54, "str2")).toDF("colstr1", "colnum", "colstr2")

      val n = 2
      increaseNLevels(n)(df)
    }

    List(
      Lens("colstr1") _,
      Lens("level.colstr1") _,
      Lens("level.level.colstr1") _
    ).foreach(proto => {
      val lens = proto(leveleddf.schema)
      getSet(lens, leveleddf)
      setGet(lens, leveleddf, "aaa")
    })

    List(
      Lens("colnum") _,
      Lens("level.colnum") _,
      Lens("level.level.colnum") _
    ).foreach(proto => {
      val lens = proto(leveleddf.schema)
      getSet(lens, leveleddf)
      setGet(lens, leveleddf, 12)
    })
  }

}
