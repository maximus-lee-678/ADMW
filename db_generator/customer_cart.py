import defs
import pd_reorder
import random
from datetime import datetime
from pathlib import Path
import pandas as pd


def main(CUSTOMERS_TO_SPAWN):
    """
    Generates a CSV file containing customer cart details sorted randomly. 
    If a customer has a cart, their id will occur sequentially in the file.
    There is a 10% chance that a customer will have a cart.
    If a customer has a cart, number of items is - 50% 1-5, 25% 6-10, 20% 11-15, 5% 16-25

    The file contains the following columns:
    - id
    - id_cust: id of the customer
    - id_prod: id of a random product
    - quantity: quantity of the product - 90% 1, 8% 2-4, 2% 5-8
    """

    df_customer = pd.read_csv(defs.FILE_CUSTOMER_DETAILS_ORDERED)
    df_product = pd.read_csv(defs.FILE_PRODUCT_MISSING_FIELDS)

    id_counter = 1
    with open(defs.FILE_CUSTOMER_CART_ORDERED, "w") as f:
        f.write("id,id_cust,id_prod,quantity\n")

        for i, row in df_customer.iterrows():
            product_list = get_product_list(df_product)
            for id_prod in product_list:
                f.write(f"{id_counter},{row["id"]},{id_prod},{get_weighted_cart_item_quantity()}\n")
                id_counter += 1

                if (i + 1) % 10000 == 0:
                    print(f"[{datetime.now()}] Generated {i + 1} customers' cart details.")

    print(f"[{datetime.now()}] Generated cart for {CUSTOMERS_TO_SPAWN} customers. ({id_counter} rows)")

    df_customer_cart_non_random = pd.read_csv(defs.FILE_CUSTOMER_CART_ORDERED, keep_default_na=False)
    df_customer_cart_final = pd_reorder.randomise_customer_cart(df_customer_cart_non_random)
    df_customer_cart_final.to_csv(defs.FILE_CUSTOMER_CART_RANDOM, index=False)
    Path(defs.FILE_CUSTOMER_CART_ORDERED).unlink(missing_ok=False)
    print(f"[{datetime.now()}] Randomised cart order.")


def get_product_list(df_product):
    """
    Returns either a list of product ids (10%), or an empty list (90%).
    A list of product ids will contain item counts of - 50% 1-5, 25% 6-10, 20% 11-15, 5% 16-25.

    :param df_product: DataFrame containing product details.
    """

    def get_weighted_cart_item_count():
        """
        Returns a random number, with distribution of - 50% 1-5, 25% 6-10, 20% 11-15, 5% 16-25.
        """

        random_cart_num = random.randint(0, 100)
        # 50% 1-5
        # 25% 6-10
        # 20% 11-15
        # 5% 16-25
        if 0 <= random_cart_num <= 50:
            return random.randint(1, 5)
        elif 51 <= random_cart_num <= 75:
            return random.randint(6, 10)
        elif 76 <= random_cart_num <= 95:
            return random.randint(11, 15)
        else:
            return random.randint(16, 25)

    if random.randint(0, 10) == 0:  # 10% chance of having a cart
        return df_product.sample(n=get_weighted_cart_item_count())["id"].tolist()
    else:
        return []


def get_weighted_cart_item_quantity():
    """
    Returns a random number with distribution of - 90% 1, 8% 2-4, 2% 5-8.
    """

    random_cart_num = random.randint(0, 100)
    # 90% 1
    # 8% 2-4
    # 2% 5-8
    if 0 <= random_cart_num <= 90:
        return 1
    elif 91 <= random_cart_num <= 98:
        return random.randint(2, 4)
    else:
        return random.randint(5, 8)


if __name__ == "__main__":
    main()
