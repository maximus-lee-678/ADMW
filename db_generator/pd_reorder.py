import pandas as pd


def main():
    """
    Contains functions that are used by other scripts to reorder dataframes to create more convincing data.

    This file is not meant to be called directly.
    """
    
    print("Not meant to be called directly!")


def sort_customer(df_customer):
    """
    Used by customer.py to sort customer details by created_at date.

    :param df_customer: DataFrame containing customer details.
    """

    df_customer_ordered = df_customer.sort_values(by="created_at")

    df_customer_ordered = df_customer_ordered.drop(["id"], axis=1)

    # zeroise index, drop=True needed to not save as column immediately
    df_customer_ordered = df_customer_ordered.reset_index(drop=True)
    df_customer_ordered.index += 1
    df_customer_ordered.insert(loc=0, column="id", value=df_customer_ordered.index)

    return df_customer_ordered


def randomise_customer_cart(df_customer_cart_non_random):
    """
    Used by customer_cart.py to randomise the order of customer ids in the customer cart.

    :param df_customer_cart_non_random: DataFrame containing customer cart details.
    """

    df_customer_cart_final = df_customer_cart_non_random.sample(frac=1)

    df_customer_cart_final = df_customer_cart_final.drop(["id"], axis=1)

    df_customer_cart_final = df_customer_cart_final.reset_index(drop=True)
    df_customer_cart_final.index += 1
    df_customer_cart_final.insert(loc=0, column="id", value=df_customer_cart_final.index)

    return df_customer_cart_final


def order_customer_purchase_instance(df_customer_purchase_instance_ordered_by_id):
    """
    DEPRECATED METHOD: not used as old implementation caused program to freeze on large datasets.
    Used by customer_transaction.py to sort customer purchase instance details by created_at date.
    In addition to the sorted dataframe, this function also returns a list containing ids in the order they occur in the new dataframe, 
    exclusively for use by order_customer_purchase_instance_items.

    :param df_customer_purchase_instance_ordered_by_id: DataFrame containing customer purchase instance details.
    """

    df_customer_purchase_instance_final = df_customer_purchase_instance_ordered_by_id.sort_values(by="created_at")

    # store new order of ids for use in ordering purchase instance items
    new_id_order = df_customer_purchase_instance_final["id"].tolist()

    df_customer_purchase_instance_final = df_customer_purchase_instance_final.drop(["id"], axis=1)

    df_customer_purchase_instance_final = df_customer_purchase_instance_final.reset_index(drop=True)
    df_customer_purchase_instance_final.index += 1
    df_customer_purchase_instance_final.insert(loc=0, column="id", value=df_customer_purchase_instance_final.index)

    return df_customer_purchase_instance_final, new_id_order


def order_customer_purchase_instance_items(df_customer_purchase_instance_items_ordered_by_id, new_id_order):
    """
    DEPRECATED METHOD: not used as old implementation caused program to freeze on large datasets.
    Used by customer_transaction.py to sort customer purchase instance items by the order they appear in the new customer purchase instance dataframe
    created by order_customer_purchase_instance.

    :param df_customer_purchase_instance_items_ordered_by_id: DataFrame containing customer purchase instance items details.
    :param new_id_order: List containing ids in the order they occur in the new customer purchase instance dataframe.
    """

    df_customer_purchase_instance_items_final = df_customer_purchase_instance_items_ordered_by_id.sort_values(
        by="id_inst", key=lambda column: column.map(lambda e: new_id_order.index(e)))
    # re-index id_inst in the order they appear
    df_customer_purchase_instance_items_final['id_inst'] = pd.factorize(df_customer_purchase_instance_items_final['id_inst'])[0] + 1

    df_customer_purchase_instance_items_final = df_customer_purchase_instance_items_final.drop(["id"], axis=1)

    df_customer_purchase_instance_items_final = df_customer_purchase_instance_items_final.reset_index(drop=True)
    df_customer_purchase_instance_items_final.index += 1
    df_customer_purchase_instance_items_final.insert(loc=0, column="id", value=df_customer_purchase_instance_items_final.index)

    return df_customer_purchase_instance_items_final


if __name__ == "__main__":
    main()
