import aio_pika
import logging

async def produce_messages_to_rmq(num_of_messages: int =1000):

    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")

    # Creating a channel
    async with connection:
        channel = await connection.channel()

        # Declare a queue
        queue = await channel.declare_queue("my_queue", durable=True)

        # Sending messages
        for i in range(num_of_messages):
            message = aio_pika.Message(body=f'Message {i}'.encode())
            await channel.default_exchange.publish(message, routing_key=queue.name)
            logging.info(f"Sent Message {i}")

        logging.info("All messages sent")