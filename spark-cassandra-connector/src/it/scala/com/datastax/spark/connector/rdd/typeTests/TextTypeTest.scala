package com.datastax.spark.connector.rdd.typeTests

class TextTypeTest extends AbstractTypeTest[String, String] {
  override val typeName = "text"
  override val typeData: Seq[String] = Seq("काचं शक्नोम्यत्तुम् । नोपहिनस्ति माम् ॥", "⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑", "אני יכול לאכול זכוכית וזה לא מזיק לי.", " நான் கண்ணாடி சாப்பிடுவேன், அதனால் எனக்கு ஒரு கேடும் வராது.", " ᠪᠢ ᠰᠢᠯᠢ ᠢᠳᠡᠶᠦ ᠴᠢᠳᠠᠨᠠ ᠂ ᠨᠠᠳᠤᠷ ᠬᠣᠤᠷᠠᠳᠠᠢ ᠪᠢᠰᠢ ")
  override val typeSet: Set[String] = Set("elementA☢", "elementB☢", "elementC☢")
  override val typeMap1: Map[String, String] = Map("key1" -> "☢", "key2" -> "☎", "key3" -> "♎")
  override val typeMap2: Map[String, String] = Map("key1☢" -> "val1", "key2☢" -> "val2", "key3☢" -> "val3")

  override val addData: Seq[String] = Seq(" ᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜", "I kaun Gloos essen, es tuat ma ned weh.", " Meg tudom enni az üveget, nem lesz tőle bajom", "Можам да јадам стакло, а не ме штета.", "Կրնամ ապակի ուտել և ինծի անհանգիստ չըներ։")
  override val addSet: Set[String] = Set("elementD☢", "elementE☢", "elementF☢")
  override val addMap1: Map[String, String] = Map("key4" -> "val☢4", "key5" -> "val☢5", "key3" -> "val☢6")
  override val addMap2: Map[String, String] = Map("key☢4" -> "val4", "key☢5" -> "val5", "key☢3" -> "val6")


  override def getDriverColumn(row: com.datastax.driver.core.Row, colName: String): String = {
    row.getString(colName)
  }
}

