package com.game24point.rg

object Game24 {
  def main(args: Array[String]): Unit = {

    println(eval("23+234+234"))
  }

  def eval(str:String):Int = str match{

    case Add(expr1,expr2) => eval(expr1)+eval(expr2)
    case _ => str toInt
  }
}

//先写一个加
object Add{

  def unapply(str: String): Option[(String,String)] ={

    val index = str indexOf("+")
    if(index == -1) None
    else
      Some(str.substring(0,index),str.substring(index+1))
  }
}