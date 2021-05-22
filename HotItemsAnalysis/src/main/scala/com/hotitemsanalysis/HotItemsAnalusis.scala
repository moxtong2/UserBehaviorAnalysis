package com.hotitemsanalysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer


/**
 * 用户点击商品的样目类
 *
 * @param userId     用户id
 * @param itemId     商品id
 * @param categoryId 商品类目
 * @param behavior   用户行为类型，包括(‘pv’, ‘’buy, ‘cart’, fav’)
 * @param timestamp  行为发生的时间戳，单位秒
 */
case class UserBehavior(userId: Long, itemId: Long, categoryId: Long, behavior: String, timestamp: Long)

/**
 * 定义商品热门排行榜样目类 （最后可以一目了然 看到的结果结构）
 *
 * @param itemId    商品id
 * @param windowEnd 窗后最后关闭时间
 * @param count     点击次数
 */
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

/**
 * 热点分析
 */
object HotItemsAnalusis {

  def main(args: Array[String]): Unit = {
    /**
     * 环境搭建
     */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //设置并行度
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //定义时间语义

    //读取文件 并转换成样目类
    val inputData = "\\Users\\zhangtongtong\\Documents\\gitWork\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv"
    val inputDataStream = env.readTextFile(inputData)
    val dataStream = inputDataStream.map(
      data => {
        //进行数据的拆分
        val splitArray = data.split(",")
        //将文件中的数据进行包装
        UserBehavior(splitArray(0).toLong, splitArray(1).toLong, splitArray(2).toLong, splitArray(3), splitArray(4).toLong)
      })
    //设置事件时间需要指定一个事件时间（这个时间一般是指数据中的时间） 分配一个升序的时间戳
    .assignAscendingTimestamps(_.timestamp * 1000L)

    //得到设置时间戳后DataStream 加载到样目类中
    //得到窗口聚合结果
    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv") //过滤PV行为
      .keyBy("itemId") //以商品id的数据进行拆分分组
      .timeWindow(Time.hours(1), Time.minutes(5)) //设置窗口
      .aggregate(new CountAgg(), new ItemViewWindowResult()) //聚合 自定义两个函数

    val resultStream: DataStream[String] = aggStream
      .keyBy("windowEnd") //按照窗口分组，收集当前窗口内的商品count数据
      .process(new TopNHotItems(5)) //自定义处理流程

    resultStream.print()

    env.execute("Hot Items analusis test")

  }

}


class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  /**
   * 初始化计数器
   * 创建一个累加器
   *
   * @return
   */
  override def createAccumulator(): Long = 0L

  /**
   * 每来一条数据进行add ，count值加 1
   *
   * @param value       要添加的值
   * @param accumulator 要向其中添加值的累加器
   * @return 累加器的类型（中间聚合状态）。
   */
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  /**
   * 返回累加器的聚合结果
   *
   * @param accumulator
   * @return
   */
  override def getResult(accumulator: Long): Long = accumulator

  /**
   * 合并累加器
   * 参数： a–要合并的累加器 ，b–另一个要合并的累加器 返回：合并状态的累加器
   * 一般是 SESSIONWOINDOWS 会话窗口定义
   *
   * @param a
   * @param b
   * @return
   */
  override def merge(a: Long, b: Long): Long = a + b
}

class ItemViewWindowResult extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  /**
   *
   * @param key    为其计算此窗口的键。
   * @param window 正在评估的窗口。
   * @param input  输入
   * @param out    输出
   */
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val iteamId = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(iteamId, windowEnd, count))
  }
}

/**
 * 自定义 KeyedProcessFunction
 *
 * @param topsize 排行商品个数
 */
class TopNHotItems(topsize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
  //定义状态 ：listState
  private var itemViewCountlistState:ListState[ItemViewCount]=_

  override def open(parameters: Configuration): Unit = {
    itemViewCountlistState =getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCountlistState-list", classOf[ItemViewCount]))
  }
  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //每来一条数 直接加入 list
    itemViewCountlistState.add(value)
    println(value)
    //注册一个定时器 一毫秒之后
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //为了方便排序 另外定义一个ListBffer 保存listState 中的所有数据
    val allItemViewCounts :ListBuffer[ItemViewCount]= ListBuffer()
    val iter= itemViewCountlistState.get().iterator()
    while (iter.hasNext){
      allItemViewCounts += iter.next()
    }
    //清空状态
    itemViewCountlistState.clear()

    //按照count值大小排序 取前n个
    val sortedItemViewCount=allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topsize)

    //将排名信息转成String
    val result:StringBuilder=new StringBuilder

    result.append("窗口结束时间 ：").append(new  Timestamp(timestamp - 1)).append("\n")

    //遍历结果集 输出到一行
    for(i <- sortedItemViewCount.indices ){
      val currentItemViewCount=sortedItemViewCount(i)
      result.append("NO").append(i + 1).append(":\t")
        .append("商品ID= ").append(currentItemViewCount.itemId).append("\t")
        .append("热门度= ").append(currentItemViewCount.count).append("\n")
    }
    result.append("=================================\n\n")

    Thread.sleep(1000)

    out.collect(result.toString())
  }


}