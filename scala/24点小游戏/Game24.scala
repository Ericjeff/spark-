package com.game24point.rg

import scala.collection.mutable

object Game24 {
  def main(args: Array[String]): Unit = {

    println(eval("23+234+234-234/234+234*234"))
    println(eval("23+234+(234-231)/(234+234)*234"))
    println(eval("234/25"))
  }

  //模式匹配提取
  def eval(str:String):Rational = str match{
    case Bracket(part1,expr,part2) => eval(part1+eval(expr)+part2)
    case Add(expr1,expr2) => eval(expr1)+eval(expr2)
    case Subtract(expr1,expr2) => eval(expr1)-eval(expr2)
    case Multiply(expr1,expr2) => eval(expr1)*eval(expr2)
    case Divide(expr1,expr2) => eval(expr1)/eval(expr2)
    case _ => new Rational(str.toInt,1)
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


//实现括号
// 分为(part1 expr part2)三个部分
// 如：1+23+(34*234)+34 => 1+23  34*234  34
//使用栈
object Bracket{

  //找出括号的下标
  def matchBracket(str:String):Option[(Int,Int)]={

    val left = str.indexOf("(")

    if(left >=0){

      val remaining = str.substring(left+1)
      var index = 0
      var right = 0

      val stack = mutable.Stack[Char]()
      for(c <- remaining if right == 0){
        index += 1
        c match{
          case '(' => stack push(c)
          case ')' => if(stack isEmpty) right=left+index else stack pop
          case _ =>
        }
      }
      Some(left,right)
    }else None
  }

  def unapply(str: String): Option[(String,String,String)] ={

    Bracket.matchBracket(str) match{
      case Some((left:Int,right:Int)) =>{
        val part1 = if(left==0) "" else str.substring(0,left)
        val expr = str.substring(left+1,right)
        val part2 = if(right == str.length-1) "" else str.substring(right+1)
        Some(part1,expr,part2)
      }
      case _ => None
    }
  }
}

//实现精确求值,任何有理数都可以转化为分数
// 使用分数代替整数

class Rational(n:Int,d:Int){

  require(d!=0)

  private val g = gcd(n,d)
  val nomer = n/g
  val denom = d/g

  def +(that:Rational)={
    new Rational(
      nomer*that.denom+denom*that.nomer,
      denom*that.denom
    )
  }

  def -(that:Rational)={
    new Rational(
      nomer*that.denom-denom*that.nomer,
      denom*that.denom
    )
  }

  def /(that:Rational)={
    new Rational(
      nomer*that.denom,
      denom*that.nomer
    )
  }

  def *(that:Rational)={
    new Rational(
      nomer*that.nomer,
      denom*that.denom
    )
  }

  private def gcd(a:Int,b:Int):Int={
    if(b==0) a else gcd(b,a%b)
  }

  def this(n:Int) = this(n,1)
  override def toString(): String = nomer +"/" + denom
}