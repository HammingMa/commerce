package com.atguigu.session.app

case class SortKey(clickCount: Long, orderCount: Long, payCount: Long) extends Ordered[SortKey] {
  override def compare(that: SortKey): Int = {
    var result: Int = 0
    if (this.clickCount - that.clickCount != 0) {
      result = (this.clickCount - that.clickCount).toInt
    } else if (this.orderCount - that.orderCount != 0) {
      result = (this.orderCount - that.orderCount).toInt
    } else if (this.payCount - that.payCount != 0) {
      result = (this.payCount - that.payCount).toInt
    }

    result
  }
}
