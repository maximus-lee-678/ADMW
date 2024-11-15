import defs
import pd_reorder
import random
from datetime import datetime, timedelta
import decimal
import pandas as pd
from pathlib import Path
import time

class PRODUCT_ID_DICT_KEYS:
    ID_LOW = 1
    ID_MED = 2
    ID_HIGH = 3
    ID_VHIGH = 4


class CUSTOMER_POINT_TYPES:
    ID_NON_MEMBER = 0
    ID_NORMAL = 1
    ID_RANDOM = 2
    ID_HOARDER = 3


def main(CUSTOMERS_TO_SPAWN):
    """
    Generates 2 CSV files:
    1. containing customer purchase instance details sorted by created_at date.
    2. containing customer purchase instance items details sorted by customer purchase instance's ordering.

    The files contain the following columns:
    File 1 (customer_purchase_instance):
    - id: id
    - id_cust: id of the customer
    - points_gained: points gained from the transaction, 10% of the price of the transaction
    - points_used: points used in the transaction
    - created_at: random date between customer's created_at date and now

    File 2 (customer_purchase_instance_items):
    - id: id
    - id_inst: id of the customer purchase instance
    - id_prod: id of a random product
    - quantity: quantity of the product
    - price: price of the product * quantity
    """

    df_customer = pd.read_csv(defs.FILE_CUSTOMER_DETAILS_ORDERED, keep_default_na=False)
    df_product = pd.read_csv(defs.FILE_PRODUCT_MISSING_FIELDS, keep_default_na=False)
    product_pricing_dict, ranges_id_list_dict = get_product_prices(df_product)

    id_counter_purchase = 1
    id_counter_purchase_items = 1
    customer_points_dict = {}
    with open(defs.FILE_CUSTOMER_PURCHASE_INSTANCE_UNORDERED, "w") as f_purchase, open(defs.FILE_CUSTOMER_PURCHASE_INSTANCE_ITEMS_UNORDERED, "w") as f_purchase_items:
        f_purchase.write("id,id_cust,points_gained,points_used,created_at\n")
        f_purchase_items.write("id,id_inst,id_prod,quantity,price\n")

        # each customer
        for index, customer in df_customer.iterrows():
            points = 0
            customer_personality = get_customer_type(customer["member_card_sn"] != "null")
            random_datetimes = get_random_datetimes(customer["member_card_sn"] != "null", customer["created_at"])

            # each transaction
            for transaction_index in range(0, len(random_datetimes)):
                point_gain = decimalise_value(0)
                price = decimalise_value(0)

                # each item
                id_quant_price_list = get_weighted_transaction_items(product_pricing_dict, ranges_id_list_dict)
                for item in id_quant_price_list:
                    f_purchase_items.write(f"""{id_counter_purchase_items},\
{id_counter_purchase},\
{item["id"]},\
{item["quantity"]},\
{item["price"]}\n""")
                    price += decimalise_value(item["price"] * item["quantity"])
                    id_counter_purchase_items += 1

                point_gain += price_to_points(customer["member_card_sn"] != "null", price)
                points = points + point_gain
                point_deduction = get_customer_point_deduction(customer_personality, points, price)
                points = points - point_deduction

                f_purchase.write(f"""{id_counter_purchase},\
{customer["id"]},\
{point_gain},\
{point_deduction},\
{random_datetimes[transaction_index]}\n""")
                customer_points_dict[customer["id"]] = str(points)

                id_counter_purchase += 1

            if customer["id"] not in customer_points_dict:
                customer_points_dict[customer["id"]] = str(decimalise_value(0))

            if (index+1) % 10000 == 0:
                print(f"[{datetime.now()}] Generated {index+1} customers' transactions.")

    print(f"[{datetime.now()}] Generated transactions for {CUSTOMERS_TO_SPAWN} customers.")

    for index, row in df_customer.iterrows():
        df_customer.loc[index, "points"] = customer_points_dict[row["id"]]

    df_customer.to_csv(defs.FILE_CUSTOMER_DETAILS_ORDERED, index=False)
    print(f"[{datetime.now()}] Updated customer points.")

    df_customer_purchase_instance_ordered_by_id = pd.read_csv(defs.FILE_CUSTOMER_PURCHASE_INSTANCE_UNORDERED, keep_default_na=False)
    df_customer_purchase_instance_items_ordered_by_id = pd.read_csv(
        defs.FILE_CUSTOMER_PURCHASE_INSTANCE_ITEMS_UNORDERED, keep_default_na=False)

    df_customer_purchase_instance_final, new_id_order = pd_reorder.order_customer_purchase_instance(
        df_customer_purchase_instance_ordered_by_id)
    df_customer_purchase_instance_final.to_csv(defs.FILE_CUSTOMER_PURCHASE_INSTANCE_ORDERED, index=False)
    Path(defs.FILE_CUSTOMER_PURCHASE_INSTANCE_UNORDERED).unlink(missing_ok=False)
    print(f"[{datetime.now()}] Sorted customer purchase instance by created_at.")

    df_customer_purchase_instance_items_final = pd_reorder.order_customer_purchase_instance_items(
        df_customer_purchase_instance_items_ordered_by_id, new_id_order)
    df_customer_purchase_instance_items_final.to_csv(defs.FILE_CUSTOMER_PURCHASE_INSTANCE_ITEMS_ORDERED, index=False)
    Path(defs.FILE_CUSTOMER_PURCHASE_INSTANCE_ITEMS_UNORDERED).unlink(missing_ok=False)
    print(f"[{datetime.now()}] Sorted customer purchase instance items by customer purchase instance's ordering.")


def decimalise_value(value):
    """
    Returns a decimal.Decimal value of the input value, rounded down to 2 decimal places.

    Used by any calculations that involve points or prices.
    
    :param value: The value to be converted to decimal.Decimal.
    """
    return decimal.Decimal(value).quantize(decimal.Decimal("0.00"), rounding=decimal.ROUND_FLOOR)


def get_product_prices(df_product):
    """
    Returns a dictionary of product ids (variable 1) and their prices and a dictionary of product ids in price ranges (variable 2).

    First dictionary has key names: product ids and values: prices.

    Second dictionary has key names: PRODUCT_ID_DICT_KEYS.ID_LOW, PRODUCT_ID_DICT_KEYS.ID_MED, 
    PRODUCT_ID_DICT_KEYS.ID_HIGH, PRODUCT_ID_DICT_KEYS.ID_VHIGH and contains lists of product ids.
    Low contains products with prices <= 10, Med contains products with prices 10.01-25, 
    High contains products with prices 25.01-50, VHigh contains products with prices 50.01-infinity.

    Used exclusively by get_weighted_transaction_items.

    :param df_product: DataFrame containing product details.
    """

    product_pricing_dict = dict(zip(df_product["id"], df_product["price"]))
    ranges_id_list_dict = {
        PRODUCT_ID_DICT_KEYS.ID_LOW: [],
        PRODUCT_ID_DICT_KEYS.ID_MED: [],
        PRODUCT_ID_DICT_KEYS.ID_HIGH: [],
        PRODUCT_ID_DICT_KEYS.ID_VHIGH: []
    }

    # 0-10
    # 10.01-25
    # 25.01-50
    # 51-infinity
    for key, value in product_pricing_dict.items():
        if value <= 10:
            ranges_id_list_dict[PRODUCT_ID_DICT_KEYS.ID_LOW].append(key)
        elif 10.01 <= value <= 25:
            ranges_id_list_dict[PRODUCT_ID_DICT_KEYS.ID_MED].append(key)
        elif 25.01 <= value <= 50:
            ranges_id_list_dict[PRODUCT_ID_DICT_KEYS.ID_HIGH].append(key)
        else:
            ranges_id_list_dict[PRODUCT_ID_DICT_KEYS.ID_VHIGH].append(key)

    return product_pricing_dict, ranges_id_list_dict


def get_customer_type(is_member):
    """
    Returns a customer personality type based on whether the customer is a member or not.
    If the customer is not a member, 
    returns CUSTOMER_POINT_TYPES.ID_NON_MEMBER (never uses points).

    If the customer is a member, returns:

    CUSTOMER_POINT_TYPES.ID_NORMAL (uses points immediately) 50% of the time,
    CUSTOMER_POINT_TYPES.ID_RANDOM (25% chance to use points per transaction) 49% of the time,
    CUSTOMER_POINT_TYPES.ID_HOARDER (never uses points) 1% of the time.

    Used by main method to get a random customer type.

    :param is_member: Whether the customer is a member or not.
    """
    if not is_member:
        return CUSTOMER_POINT_TYPES.ID_NON_MEMBER

    random_point_spender_roll = random.randint(0, 100)
    # 50% use points immediately
    # 49% have a 25% random chance to use as much as possible
    # 1% dont use at all
    if 0 <= random_point_spender_roll <= 50:
        return CUSTOMER_POINT_TYPES.ID_NORMAL
    elif 51 <= random_point_spender_roll <= 99:
        return CUSTOMER_POINT_TYPES.ID_RANDOM
    else:
        return CUSTOMER_POINT_TYPES.ID_HOARDER


def get_random_datetimes(is_member, customer_created_at):
    """
    Returns a list of random datetimes between the customer's created_at date and now.
    For each day, there is a 3/5% chance that that day is added to the list. Time of day is random.
    The chance is 3% if the customer is not a member, 5% if the customer is not a member.

    Used by main method to get a list of random transaction datetimes for each customer.

    :param is_member: Whether the customer is a member or not.
    :param customer_created_at: The date the customer was created at.
    """
    
    start_datetime = datetime.strptime(customer_created_at, "%Y-%m-%d %H:%M:%S")
    end_datetime = datetime.now()
    purchase_chance = 0.05 if is_member else 0.03

    random_datetimes = []

    current_datetime = start_datetime
    while current_datetime <= end_datetime:
        if random.random() <= purchase_chance:
            random_datetime = current_datetime.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )
            random_datetimes.append(random_datetime)
        current_datetime += timedelta(days=1)

    return random_datetimes


def get_weighted_transaction_items(product_pricing_dict, ranges_id_list_dict):
    """
    Returns a list of dictionaries with keys "id", "price" and "quantity" for each item in the transaction.
    The number of items in the list is random, with distribution of - 30% 1-3 items, 40% 4-6 items, 25% 7-10 items, 5% 11-20 items.
    Each item has an equal chance of being in the low, medium, high or very high price range.
    Each item has a random quantity with distribution of - 93% 1, 4% 2, 2% 3, 1% 4.

    Used by main method to get a list of random transaction items for each transaction.

    :param product_pricing_dict: Dictionary containing product ids and their prices generated by get_product_prices.
    :param ranges_id_list_dict: Dictionary containing product ids grouped by price ranges generated by get_product_prices.
    """

    def get_weighted_item_quantity():
        """
        Returns a random number with distribution of - 93% 1, 4% 2, 2% 3, 1% 4.
        """

        random_item_quantity_roll = random.randint(0, 100)
        # 93% 1
        # 4% 2
        # 2% 3
        # 1% 4
        if 0 <= random_item_quantity_roll <= 93:
            return 1
        elif 94 <= random_item_quantity_roll <= 97:
            return 2
        elif 98 <= random_item_quantity_roll <= 99:
            return 3
        else:
            return 4

    random_transaction_items_roll = random.randint(0, 100)
    # 30% 1-3 items
    # 40% 4-6 items
    # 25% 7-10 items
    # 5% 11-20 items
    if 0 <= random_transaction_items_roll <= 30:
        num_items = random.randint(1, 3)
    elif 31 <= random_transaction_items_roll <= 70:
        num_items = random.randint(4, 6)
    elif 71 <= random_transaction_items_roll <= 95:
        num_items = random.randint(7, 10)
    else:
        num_items = random.randint(11, 20)

    id_quant_price_list = []
    for _ in range(0, num_items):
        random_price_range_roll = random.randint(0, 3)
        match random_price_range_roll:
            case 0:
                random_id = random.choice(ranges_id_list_dict[PRODUCT_ID_DICT_KEYS.ID_LOW])
            case 1:
                random_id = random.choice(ranges_id_list_dict[PRODUCT_ID_DICT_KEYS.ID_MED])
            case 2:
                random_id = random.choice(ranges_id_list_dict[PRODUCT_ID_DICT_KEYS.ID_HIGH])
            case 3:
                random_id = random.choice(ranges_id_list_dict[PRODUCT_ID_DICT_KEYS.ID_VHIGH])

        id_quant_price_list.append({
            "id": random_id,
            "price": product_pricing_dict[random_id],
            "quantity": get_weighted_item_quantity()
        })

    return id_quant_price_list


def price_to_points(is_member, price):
    """
    Returns price / 10 if the customer is a member, 0.00 otherwise.

    Used by main method to calculate the points gained from a transaction.

    :param is_member: Whether the customer is a member or not.
    :param price: The price of the transaction
    """

    # 10% point rebate
    return decimalise_value(price / 10) if is_member else decimalise_value(0)


def get_customer_point_deduction(personality, points, price):
    """
    Returns how many points a customer will use in a transaction based on their personality type.
    If the customer is not a member, returns 0.
    If the customer is a member, returns:
    NORMAL: tries to use as many points as possible, but not more than the price. if points < price, uses all points.
    RANDOM: 25% chance of using as many points as possible, but not more than the price. if points < price, uses all points. 75% chance of using 0 points.
    HOARDER: uses 0 points.

    Used by main method to calculate the points used in a transaction.

    :param personality: The personality type of the customer.
    :param points: The points the customer has.
    :param price: The price of the transaction.
    """

    if personality == CUSTOMER_POINT_TYPES.ID_NON_MEMBER:
        return 0
    # 50% use points immediately
    # 49% have a 25% random chance to use as much as possible
    # 1% dont use at all
    elif personality == CUSTOMER_POINT_TYPES.ID_NORMAL:
        return decimalise_value(min(points, price))
    elif personality == CUSTOMER_POINT_TYPES.ID_RANDOM:
        if random.randint(0, 3) == 0:  # 25% chance of using as much as possible
            return decimalise_value(min(points, price))
        else:
            return 0
    else:
        return 0


if __name__ == "__main__":
    main()
