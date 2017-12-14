package com.movie

import scala.collection.Map
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

//电影评价
case class MovieRating(userID:String,movieID:Int,rating:Double) extends scala.Serializable


object Recommend {

  def main(args: Array[String]): Unit = {

    //创建sc
    val conf = new SparkConf().setAppName("recommend").setMaster("local")
    val sc = new SparkContext(conf)

    //导入原始数据
    val base = "file:///G:\\bigdata\\project\\douban-recommender\\data\\"
    val rawUserMoviesData = sc.textFile(base+"user_movies.csv")
    val rawMoviesData = sc.textFile(base+"hot_movies.csv")

    //准备数据
    preparation(rawUserMoviesData,rawMoviesData)

    //推荐模型训练
    val model = trainModel(sc,rawUserMoviesData,rawMoviesData)

    //获取推荐列表
    //recommendMovies(sc,model,rawUserMoviesData,rawMoviesData,5,base)

    //获取物品推荐
    recommendGoods(model,rawMoviesData)
  }

  //获取电影名字
  def buildMovie(rawMoviesData:RDD[String]): Map[Int,String]= {
    rawMoviesData.flatMap{line =>
      val tokens = line.split(",")
      if(tokens(0).isEmpty()){
        None
      }else{
        Some((tokens(0).toInt,tokens(2)))
      }
    }.collectAsMap()
  }

  //获取电影评价
  def buildRating(rawUserMoviesData:RDD[String]):RDD[MovieRating]={
    rawUserMoviesData.map{line =>
      val Array(userID,moviesID,countStr) = line.split(",").map(_.trim)
      var count = countStr.toInt
      count = if(count == -1) 3 else count

      MovieRating(userID,moviesID.toInt,count)
    }
  }

  //分析清理数据
  def preparation(rawUseMoviesData:RDD[String],
                  rawMoviesData:RDD[String])={

    val userIDStats = rawUseMoviesData.mapPartitions(x=>{
      x.map(_.split(",")(0).trim)
    }).distinct().zipWithUniqueId().map(_._2.toDouble).stats()

    val itemIDStats = rawUseMoviesData.map(_.split(",")(1).trim.toDouble).distinct().stats()

    println(userIDStats)
    println(itemIDStats)

    val moviesAndName = buildMovie(rawMoviesData);
    val (movieID,movieName) = moviesAndName.head
    println(movieID+" -> "+movieName)
  }

  //训练模型
  def trainModel(sc:SparkContext,
                rawUserMoviesData:RDD[String],
                rawMoviesData:RDD[String]):MatrixFactorizationModel={

    //将用户名转化key/value
    // 将用户名转为Int类型 去满足在ALS训练模型对类型的要求
    val data = buildRating(rawUserMoviesData)
    val userIdToInt:RDD[(String,Long)] =
      data.map(_.userID).distinct().zipWithUniqueId()


    val userIDMap:Map[String,Int] =
      userIdToInt.collectAsMap().map{case(n,l)=>(n,l.toInt)}

    val bUserIDMap = sc.broadcast(userIDMap)


    //生成spark的ASL的训练类型Rating
    val rating:RDD[Rating] = data.map{r=>
      Rating(bUserIDMap.value.get(r.userID).get,r.movieID,r.rating)
    }
    rating.keyBy(_.user).lookup(11)
    //println(bUserIDMap.value.get("51140827").get)
    //使用协同过滤进行推荐算法的训练,返回一个MatrixFactorizationModel
    /**
      * rating:训练数据
      * rank:对应的是隐因子的个数，这个值设置越高越准，
      *     但是也会产生更多的计算量。一般将这个值设置为10-200
      * iterations:迭代次数
      * lambda:该参数控制正则化过程，其值越高，
      *   正则化程度就越深。一般设置为0.01
      *
      * ALS.train(rating,rank,iterations,lambda)
      */
    val model = ALS.train(rating,50,10,0.01)
    model
  }

  //推荐n个电影列表
  def recommendMovies(sc:SparkContext,
                      model:MatrixFactorizationModel,
                      rawUserMoviesData:RDD[String],
                      rawMoviesData:RDD[String],
                      movieNum:Int,
                      base:String)={

    //获取电影的名字,并且设置为广播变量
    val moviesAndName = buildMovie(rawMoviesData)
    val bMoviesAndName = sc.broadcast(moviesAndName)

    val data = buildRating(rawUserMoviesData)
    val userIdToInt = data.map(_.userID).distinct().zipWithUniqueId()

    //上面的key/value => value/key
    val reverseUserIDMapping:RDD[(Long,String)] =
      userIdToInt.map{case (l,r)=>(r,l)}

    val bReverseUserIDMap = sc.broadcast(reverseUserIDMapping.collectAsMap())

    val allRecommendations = model.recommendProductsForUsers(movieNum).map{
      case(userID,recommendations)=>
        var recommendationStr = ""
        recommendations.foreach(r=>
          recommendationStr += r.product+":"
          +bMoviesAndName.value.getOrElse(r.product,"")+",")
        if(recommendationStr.endsWith(",")){
          recommendationStr = recommendationStr.substring(0,recommendationStr.length-1)
        }
        (bReverseUserIDMap.value.get(userID).get,recommendationStr)
    }

    allRecommendations.coalesce(1).sortByKey().saveAsTextFile(base+"result1.csv")
  }

  /**
    * 物品推荐    就是给定一个物品，找到它的所有相似物品
    *  遗憾的是MLlib里面竟然没有包含内置的函数，需要自己用jblas库来实现
    *
    */

  def recommendGoods(model:MatrixFactorizationModel,
                     rawMoviesData:RDD[String])={

    //导入jblas库中的矩阵类
    import org.jblas.DoubleMatrix

    //定义相似函数,余弦相似定义
    def cosineSimilarity(vec1:DoubleMatrix,vec2:DoubleMatrix):Double={
      vec1.dot(vec2)/(vec1.norm2()*vec2.norm2())
    }

    //选定id为2973079(霍比特人3：五军之战)的电影
    val itemId = 2973079
    // 获取该物品的隐因子向量
    val itemFactor = model.productFeatures.lookup(itemId).head
    // 将该向量转换为jblas矩阵类型
    val itemVector = new DoubleMatrix(itemFactor)

    //计算与2973079的相似度
    val sims = model.productFeatures.map{
      case (id,factor)=>
        val factorVector = new DoubleMatrix(factor)
        val sim = cosineSimilarity(factorVector,itemVector)
        (id,sim)
    }

    // 获取与电影2973079最相似的10部电影
    val sortedSims = sims.top(10)(Ordering.by[(Int,Double),Double]{
      case (id,similarity)=> similarity
    })
    val movieAndName = buildMovie(rawMoviesData)
    sortedSims.foreach{case (id,similarity)=>println("name:"+movieAndName.get(id).get+"--->"+similarity)}
  }
}
