from faststream.rabbit import RabbitExchange, RabbitQueue

PAYMENTS_EXCHANGE = RabbitExchange("payments", durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", durable=True)
DLX_EXCHANGE = RabbitExchange("payments.dlx", durable=True)

PAYMENTS_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key="payments.new",
)
RETRY_QUEUE = RabbitQueue(
    "payments.retry",
    durable=True,
    routing_key="payments.retry",
    arguments={
        "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
        "x-dead-letter-routing-key": PAYMENTS_QUEUE.routing_key,
    },
)
DLQ_QUEUE = RabbitQueue(
    "payments.failed",
    durable=True,
    routing_key="payments.failed",
)
