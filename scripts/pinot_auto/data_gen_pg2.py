import psycopg2
import random
import time
from datetime import datetime, timedelta
from itertools import count


CATEGORIES = ['Electronics', 'Clothing', 'Home', 'Books', 'Toys']

PRODUCTS = {
    'Electronics': ['Phone', 'Laptop', 'Headphones'],
    'Clothing': ['Shirt', 'Pants', 'Jacket'],
    'Home': ['Sofa', 'Table', 'Chair'],
    'Books': ['Novel', 'Biography', 'Science Fiction'],
    'Toys': ['Action Figure', 'Board Game', 'Puzzle']
}


def sales_row_generator(start_id=1000):
    """
    Infinite generator producing sales rows.
    Memory safe.
    """
    for i in count(start_id):
        category = random.choice(CATEGORIES)
        product = random.choice(PRODUCTS[category])
        price = random.randint(10, 1000)
        quantity = random.randint(1, 5)
        sale_date = datetime.now() - timedelta(days=random.randint(0, 30))

        yield (
            i,
            product,
            category,
            price,
            quantity,
            sale_date,
            datetime.now()
        )


def insert_stream(batch_size=1000, sleep_seconds=0):
    """
    Continuously inserts data in batches.
    batch_size -> increase for higher throughput
    sleep_seconds -> throttle rate (0 = max speed)
    """

    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='your_password_here',
        host='YOUR_SERVER_IP',
        port='5432'
    )

    conn.autocommit = False
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO public.source_sales
        (id, product_name, category, price, quantity, sale_date, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    generator = sales_row_generator()

    print("Starting infinite insert stream...")

    while True:
        batch = [next(generator) for _ in range(batch_size)]

        cursor.executemany(insert_query, batch)
        conn.commit()

        print(f"Inserted batch of {batch_size} rows at {datetime.now()}")

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    insert_stream(
        batch_size=500,   # increase to 5000 / 10000 for heavier stress
        sleep_seconds=0.5 # set to 0 for max pressure
    )
