package com.github.jxc454.udphistogram

import com.redis._

object App {
  val r = new RedisClient("localhost", 6379)

  def main(args : Array[String]): Unit = Consumer.run(incrementRedis(r))

  def incrementRedis(r: RedisClient)(key: Int): Unit = r.hincrby("histogram", key.toString, 1)
}
