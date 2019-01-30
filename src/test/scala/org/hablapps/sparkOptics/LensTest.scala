package org.hablapps.sparkOptics

import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpecLike, Matchers}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

class LensTest extends FlatSpecLike with Matchers with SparkUtils{

  import sparkSession.implicits._

  "Spark Lens" should "modify a first level column" in {
    val df = List(
      ("str",54,"str2"),
      ("str",54,"str2"),
      ("str",54,"str2")
    ).toDF("colstr1","colnum","colstr2")

    val strLens:Lens = Lens("colstr1")(df.schema)
    val numLens:Lens = Lens("colnum")(df.schema)


    strLens.get shouldBe col("colstr1")
    val strTransformed = df.select(strLens.modify(c => concat(c, c)):_*)
    strTransformed.schema shouldBe df.schema
    strTransformed.as[(String,Int,String)].collect() shouldBe Array(("strstr",54,"str2"),("strstr",54,"str2"),("strstr",54,"str2"))

    val numTransformed = df.select(numLens.set(lit(10)):_*)
    numTransformed.schema shouldBe df.schema
    numTransformed.as[(String,Int,String)].collect() shouldBe Array(("str",10,"str2"),("str",10,"str2"),("str",10,"str2"))
  }


  it should "modify a n level column" in {
    val increaseLevel: DataFrame => DataFrame =df =>  {
      val newColumn = struct(df.schema.fields.map(f => col(f.name)):_*)
      df.withColumn("level",newColumn)
    }

    def increaseNLevels(n:Int):DataFrame => DataFrame = {
      (1 to n).map(_ => increaseLevel).reduce(_ andThen _)
    }

    val df = List(("str",54,"str2"),("str",54,"str2"),("str",54,"str2")).toDF("colstr1","colnum","colstr2")

    val n = 2
    val leveleddf = increaseNLevels(n)(df)
    val protolenteLevel: StructType => Lens = Lens("level")
    val strProt: StructType => Lens = Lens("level.level.colstr1")
    val strLens = strProt(leveleddf.schema)
    val numLens = Lens("level.level.colnum")(leveleddf.schema)

    val strTransformed = leveleddf.select(strLens.modify(c => concat(c, c)):_*)

    strTransformed.schema shouldBe leveleddf.schema
    strTransformed.select("colstr1","colnum","colstr2")
      .as[(String,Int,String)].collect() shouldBe Array(("str",54,"str2"),("str",54,"str2"),("str",54,"str2"))
    strTransformed.select("level.colstr1","level.colnum","level.colstr2")
      .as[(String,Int,String)].collect() shouldBe Array(("str",54,"str2"),("str",54,"str2"),("str",54,"str2"))
    strTransformed.select("level.level.colstr1","level.level.colnum","level.level.colstr2")
      .as[(String,Int,String)].collect() shouldBe Array(("strstr",54,"str2"),("strstr",54,"str2"),("strstr",54,"str2"))
    strTransformed.select(strLens.get).as[String].collect() shouldBe Array("strstr","strstr","strstr")


    val leveleddfoneColumn = leveleddf.select("level")
    val strTransformed2 = leveleddfoneColumn.select(strProt(leveleddfoneColumn.schema).modify(c => concat(c, c)):_*)
    strTransformed2.schema shouldBe leveleddfoneColumn.schema
    strTransformed2.select(strLens.get).as[String].collect() shouldBe Array("strstr","strstr","strstr")

    val numTransformed = leveleddf.select(numLens.modify(_ + 10):_*)
    numTransformed.schema shouldBe leveleddf.schema
    numTransformed.select("colstr1","colnum","colstr2")
      .as[(String,Int,String)].collect() shouldBe Array(("str",54,"str2"),("str",54,"str2"),("str",54,"str2"))
    numTransformed.select("colstr1", "colnum", "colstr2")
      .as[(String, Int, String)].collect() shouldBe Array(("str", 54, "str2"), ("str", 54, "str2"), ("str", 54, "str2"))
    numTransformed.select("level.colstr1", "level.colnum", "level.colstr2")
      .as[(String, Int, String)].collect() shouldBe Array(("str", 54, "str2"), ("str", 54, "str2"), ("str", 54, "str2"))
    numTransformed.select("level.level.colstr1", "level.level.colnum", "level.level.colstr2")
      .as[(String, Int, String)].collect() shouldBe Array(("str", 64, "str2"), ("str", 64, "str2"), ("str", 64, "str2"))
    numTransformed.select(numLens.get).as[Int].collect() shouldBe Array(64, 64, 64)
  }

}
