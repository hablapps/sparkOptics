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

    assertGetSet(strLens, df)
    assertSetGet(strLens, df, "aaa")
    assertGetSet(numLens, df)
    assertSetGet(numLens, df, 12)

  }

  val increaseLevel: DataFrame => DataFrame = df => {
    val newColumn = struct(df.schema.fields.map(f => col(f.name)): _*)
    df.withColumn("level", newColumn)
  }

  def increaseNLevels(n: Int): DataFrame => DataFrame = {
    (1 to n).map(_ => increaseLevel).reduce(_ andThen _)
  }
  it should "modify a n level column" in {

    val leveleddf = {
      val df = List(("str", 54, "str2"),
        ("str", 54, "str2"),
        ("str", 54, "str2")).toDF("colstr1", "colnum", "colstr2")

      val n = 2
      increaseNLevels(n)(df)
    }

    List(
      Lens("colstr1")(leveleddf.schema),
      Lens("level.colstr1")(leveleddf.schema),
      Lens("level.level.colstr1")(leveleddf.schema)
    ).foreach(lens => {
      assertGetSet(lens, leveleddf)
      assertSetGet(lens, leveleddf, "aaa")
    })

    List(
      Lens("colnum")(leveleddf.schema),
      Lens("level.colnum")(leveleddf.schema),
      Lens("level.level.colnum")(leveleddf.schema)
    ).foreach(lens => {
      assertGetSet(lens, leveleddf)
      assertSetGet(lens, leveleddf, 12)
    })
  }

  it should "change the name of the focused subcolumn" in {
    val leveleddf = {
      val df = List(("str", 54, "str2"),
        ("str", 54, "str2"),
        ("str", 54, "str2")).toDF("colstr1", "colnum", "colstr2")

      val n = 2
      increaseNLevels(n)(df)

    }
    val lensOrig = Lens("level.level.colstr1")(leveleddf.schema)
    val changed = leveleddf.select(lensOrig.rename("otherName"): _*)
    val changedLens = Lens("level.level.otherName")(changed.schema)
    changed.select(changedLens.get)
      .as[String].collect() shouldBe leveleddf.select(lensOrig.get).as[String].collect
    changed.select(changedLens.prune(): _*).schema shouldBe leveleddf.select(lensOrig.prune(): _*).schema
    changed.select(changedLens.prune(): _*).schema
  }

}
