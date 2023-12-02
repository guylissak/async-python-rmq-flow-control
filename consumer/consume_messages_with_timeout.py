import asyncio
import aio_pika
import time
import logging

class AckInfo:
    def __init__(self):
        self.last_ack_time = time.time()
        self.messages_to_ack = []


async def ack_messages(ack_info: AckInfo, interval: int):
    while True:
        await asyncio.sleep(interval)
        messages_to_ack = ack_info.messages_to_ack.copy()
        current_time = time.time()
        if messages_to_ack and (current_time - ack_info.last_ack_time) >= interval:
            logging.info(f"partial local list {len(messages_to_ack)}.")
            for msg in messages_to_ack:
                await msg.ack()
            logging.info(f"Acknowledged {len(messages_to_ack)} messages after interval.")
            ack_info.last_ack_time = time.time() 
            del ack_info.messages_to_ack[: len(messages_to_ack)]
            # await asyncio.sleep(interval)
            # In this time the global list is already get to batch_size since after acking each message the flow control lock release
            logging.info(f"After remove local messages from global messages {len(ack_info.messages_to_ack)}.")


async def consume_messages_with_timeout(queue_name, batch_size=100, ack_interval=10):

    connection = await aio_pika.connect_robust("amqp://guest:guest@127.0.0.1/")

    async with connection:

        channel = await connection.channel()

        await channel.set_qos(prefetch_count=batch_size)

        queue = await channel.declare_queue(queue_name, durable=True)

        ack_info = AckInfo()

        # Start the timer task for acknowledgment
        timer_task = asyncio.create_task(ack_messages(ack_info, ack_interval))

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                # Add the message to the batch
                ack_info.messages_to_ack.append(message)

                # Process the message (placeholder for your processing logic)
                logging.info(message.body.decode())

                # Breaking condition if specific content is found in the message
                if "stop" in message.body.decode():
                    break

        # Cancel the timer task
        logging.info("clear timer task")
        timer_task.cancel()
