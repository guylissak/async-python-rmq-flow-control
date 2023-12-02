import aio_pika

async def assert_durable_queue(queue_name: str = "my_queue"):
    # Connect to RabbitMQ
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")

    # Creating a channel
    async with connection:
        channel = await connection.channel()

        # Declare a durable queue
        queue = await channel.declare_queue(queue_name, durable=True)

        print(f"Queue '{queue.name}' declared with durable configuration")
