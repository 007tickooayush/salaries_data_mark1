package com.reazon.Analytics

import com.reazon.FileAccess.DFAO

object HighestPayRating {
  def main(args: Array[String]): Unit = {
    val dfao = new DFAO()

    dfao.getFiles.show(10)


  }

}
