import asyncio
import logging
from assert_queue import assert_durable_queue
from producer import produce_messages_to_rmq
from consumer import consume_messages_with_timeout

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    # asyncio.run(assert_durable_queue())
    # asyncio.run(produce_messages_to_rmq(num_of_messages=1000))
    asyncio.run(consume_messages_with_timeout(queue_name="my_queue", batch_size=100))