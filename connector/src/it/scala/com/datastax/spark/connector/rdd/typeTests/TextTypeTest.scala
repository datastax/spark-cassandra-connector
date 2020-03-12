package com.datastax.spark.connector.rdd.typeTests

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.spark.connector.cluster.DefaultCluster

class TextTypeTest extends AbstractTypeTest[String, String] with DefaultCluster {
  override val typeName = "text"

  override val typeData: Seq[String] = Seq("काचं शक्नोम्यत्तुम् । नोपहिनस्ति माम् ॥", "⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑", "אני יכול לאכול זכוכית וזה לא מזיק לי.", " நான் கண்ணாடி சாப்பிடுவேன், அதனால் எனக்கு ஒரு கேடும் வராது.", " ᠪᠢ ᠰᠢᠯᠢ ᠢᠳᠡᠶᠦ ᠴᠢᠳᠠᠨᠠ ᠂ ᠨᠠᠳᠤᠷ ᠬᠣᠤᠷᠠᠳᠠᠢ ᠪᠢᠰᠢ ")
  override val addData: Seq[String] = Seq(" ᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜", "I kaun Gloos essen, es tuat ma ned weh.", " Meg tudom enni az üveget, nem lesz tőle bajom", "Можам да јадам стакло, а не ме штета.", "Կրնամ ապակի ուտել և ինծի անհանգիստ չըներ։")

  override def getDriverColumn(row: Row, colName: String): String = {
    row.getString(colName)
  }
}

