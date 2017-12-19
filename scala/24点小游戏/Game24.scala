package com.game24point.rg

object Game24 {
  def main(args: Array[String]): Unit = {

    println(eval("23+234+234-234/234+234*234"))
  }

  //模式匹配提取
  def eval(str:String):Int = str match{
    case Add(expr1,expr2) => eval(expr1)+eval(expr2)
    case Subtract(expr1,expr2) => eval(expr1)-eval(expr2)
    case Multiply(expr1,expr2) => eval(expr1)*eval(expr2)
    case Divide(expr1,expr2) => eval(expr1)/eval(expr2)
    case _ => str toInt
  }
}

//加减乘除的主体
trait BinaryOp{
  val op:String
  def unapply(str:String):Option[(String,String)]={

    val index = str indexOf(op)
    if(index == -1) None
    else Some(str.substring(0,index),str.substring(index+1))
  }
}

//加减乘除实现
object Add extends {val op="+"} with BinaryOp
object Subtract extends {val op="-"} with BinaryOp
object Multiply extends {val op="*"} with BinaryOp
object Divide extends {val op="/"} with BinaryOp

