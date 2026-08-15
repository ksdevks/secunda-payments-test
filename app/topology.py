import aio_pika

from app.broker import (
    DLQ_QUEUE,
    DLX_EXCHANGE,
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    RETRY_EXCHANGE,
    RETRY_QUEUE,
)


async def declare_topology(url: str) -> None:
    connection = await aio_pika.connect_robust(url)
    try:
        channel = await connection.channel(publisher_confirms=True)
        payments_exchange = await channel.declare_exchange(
            PAYMENTS_EXCHANGE.name, aio_pika.ExchangeType.DIRECT, durable=True
        )
        retry_exchange = await channel.declare_exchange(
            RETRY_EXCHANGE.name, aio_pika.ExchangeType.DIRECT, durable=True
        )
        dlx_exchange = await channel.declare_exchange(
            DLX_EXCHANGE.name, aio_pika.ExchangeType.DIRECT, durable=True
        )
        payments_queue = await channel.declare_queue(
            PAYMENTS_QUEUE.name,
            durable=True,
        )
        retry_queue = await channel.declare_queue(
            RETRY_QUEUE.name,
            durable=True,
            arguments=RETRY_QUEUE.arguments,
        )
        dlq_queue = await channel.declare_queue(DLQ_QUEUE.name, durable=True)
        await payments_queue.bind(payments_exchange, routing_key=PAYMENTS_QUEUE.routing_key)
        await retry_queue.bind(retry_exchange, routing_key=RETRY_QUEUE.routing_key)
        await dlq_queue.bind(dlx_exchange, routing_key=DLQ_QUEUE.routing_key)
    finally:
        await connection.close()
