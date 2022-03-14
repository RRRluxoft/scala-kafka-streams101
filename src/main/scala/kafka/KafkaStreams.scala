package kafka

import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import io.circe
import io.circe.syntax.EncoderOps
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.time.Duration
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.util.Properties

object KafkaStreams {

  object Domain {
    type UserId = String
    type Profile = String
    type Product = String
    type OrderId = String
    type Status = String

    case class Order(orderId: OrderId, userId: UserId, products: List[Product], amount: BigDecimal)
    case class Discount(profile: Profile, amount: BigDecimal)
    case class Payment(orderId: OrderId, status: Status)
  }

  object Topics {
    final val OrdersByUserTopic = "orders-by-user"
    final val DiscountProfilesByUserTopic = "discount-profiles-by-user"
    final val DiscountsTopic = "discounts"
    final val OrdersTopic = "orders"
    final val PaymentsTopic = "payments"
    final val PaidOrdersTopic = "paid-orders"
  }

  import Domain._
  import Topics._
  implicit def serde[A >: Null: Decoder: Encoder]: Serde[A] = {
    val serializer = (a: A) => a.asJson.noSpaces.getBytes
    val deserializer = (bytes: Array[Byte]) => {
      val str = new String(bytes)
      decode[A](str).toOption
    }

    Serdes.fromFn[A](serializer, deserializer)
  }

  def main(args: Array[String]): Unit = {

    // topology:
    val builder = new StreamsBuilder()

    // KStream
    val userOrderStream: KStream[UserId, Order] = builder.stream[UserId, Order](OrdersByUserTopic)

    // KTable
    val userProfileTable: KTable[UserId, Profile] = builder.table[UserId, Profile](DiscountProfilesByUserTopic)

    // GlobalKTable
    val discountProfilesGTable: GlobalKTable[Profile, Discount] = builder.globalTable[Profile, Discount](DiscountsTopic)

    // KStream transformation:
    val expensiveOrders = userOrderStream.filter { (userId, order) =>
      order.amount > BigDecimal(1000)
    }

    val listOfProduct = userOrderStream.mapValues(order => order.products)
    val productsStream = userOrderStream.flatMapValues(_.products)

    // JOIN
    val ordersWithUserProfiles = userOrderStream.join(userProfileTable) { (order, profile) =>
      (order, profile)
    }

    val discountedOrderStream = ordersWithUserProfiles.join(discountProfilesGTable)(
      { case (userId, (order, profile)) => profile },
      { case ((order, profile), discount) => order.copy(amount = order.amount - discount.amount) }
    )

    // pick another identifier:
    val orderStream: KStream[OrderId, Order] = discountedOrderStream.selectKey((userId, order) => order.orderId)

    val paymentStream: KStream[OrderId, Payment] = builder.stream[OrderId, Payment](PaymentsTopic)

    val joinWindow: JoinWindows = JoinWindows.of(Duration.of(5L, ChronoUnit.MINUTES))
    val joinOrdersPayments: (Order, Payment) => Option[Order] =
      (order: Order, payment: Payment) => if (payment.status == "PAID") Option(order) else Option.empty[Order]

    val ordersPaid: KStream[OrderId, Order] = orderStream
      .join(paymentStream)(joinOrdersPayments, joinWindow)
//    .filter((orderId, maybeOrder) => maybeOrder.isDefined)
      .flatMapValues(maybeOrder => maybeOrder.toList)

    // SINK:
    ordersPaid.to(PaidOrdersTopic)

    val topology = builder.build()

    // Start KAFKA app:
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

//    println(topology.describe())
    val application = new KafkaStreams(topology, props)
    application.start()

    /**
      *    //at once for generating commands for topics creation
      *    // docker exec -it broker bash [~]
      *    List(
      *      "orders-by-user",
      *      "discount-profiles-by-user",
      *      "discounts",
      *      "orders",
      *      "payments",
      *      "paid-orders"
      *    ).foreach { topic =>
      *      println(s"kafka-topics --bootstrap-server localhost:9092 --topic $topic --create")
      *    }
      */
  }
}
