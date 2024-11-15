import defs
from datetime import datetime, timedelta
from pathlib import Path
import random
import pandas as pd


def main():
    """
    Contains functions that are used to finalise product tables.

    Adds missing active, created_at, modified_at, modified_by columns to product brand table.
    Adds missing active column to and drops url column from product category table.
    """

    df_staff = pd.read_csv(defs.FILE_STAFF_DETAILS, keep_default_na=False)
    staff_id_created_at = {int(row["id"]): row["created_at"] for _, row in df_staff.iterrows()}    # key: id, value: created_at

    Path(defs.FILE_PRODUCT_BRAND_FINAL).unlink(missing_ok=True)
    Path(defs.FILE_PRODUCT_CATEGORY_FINAL).unlink(missing_ok=True)

    df_product_details = pd.read_csv(defs.FILE_PRODUCT_MISSING_FIELDS, keep_default_na=False)
    df_product_details_final = finalise_product_details(staff_id_created_at, df_product_details)

    df_product_details_final.to_csv(defs.FILE_PRODUCT_FINAL, index=False)
    Path(defs.FILE_PRODUCT_MISSING_FIELDS).unlink(missing_ok=False)
    print(f"[{datetime.now()}] Added missing columns to product details.")

    df_product_brand = pd.read_csv(defs.FILE_PRODUCT_BRAND_MISSING_FIELDS, keep_default_na=False)
    df_product_brand_final = finalise_product_brand(staff_id_created_at, df_product_brand)

    df_product_brand_final.to_csv(defs.FILE_PRODUCT_BRAND_FINAL, index=False)
    Path(defs.FILE_PRODUCT_BRAND_MISSING_FIELDS).unlink(missing_ok=False)
    print(f"[{datetime.now()}] Added missing columns to product brands.")

    df_product_category = pd.read_csv(defs.FILE_PRODUCT_CATEGORY_WRONG_FIELDS, keep_default_na=False)
    df_product_category_final = finalise_product_category(df_product_category)

    df_product_category_final.to_csv(defs.FILE_PRODUCT_CATEGORY_FINAL, index=False)
    Path(defs.FILE_PRODUCT_CATEGORY_WRONG_FIELDS).unlink(missing_ok=False)
    print(f"[{datetime.now()}] Added missing columns to product categories.")
    print(f"[{datetime.now()}] Dropped url column from product categories.")


def finalise_product_details(staff_id_created_at, df_product_details):
    """
    Adds missing active, created_at, modified_at, modified_by columns to product details table.
    Also reorders columns to simplify importing to SQLite.

    :param staff_id_created_at: Dictionary containing staff id and created_at date.
    :param df_product_brands: DataFrame containing product brand details. 
    """

    for i, row in df_product_details.iterrows():
        df_product_details.at[i, "active"] = "1"

        staff_id, staff_created_at = random.choice(list(staff_id_created_at.items()))
        product_created_at = get_datetime_between(datetime.strptime(staff_created_at, "%Y-%m-%d %H:%M:%S"), datetime.now())

        df_product_details.at[i, "created_at"] = product_created_at.strftime("%Y-%m-%d %H:%M:%S")
        df_product_details.at[i, "modified_at"] = product_created_at.strftime(
            "%Y-%m-%d %H:%M:%S") if random.randint(0, 3) <= 2 else get_datetime_between(product_created_at, datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
        df_product_details.at[i, "modified_by"] = str(staff_id)

    # reorder columns
    df_product_details = df_product_details[[
        "id", "id_brand", "id_country", "id_category_primary", "id_category_parent_major",
        "id_category_parent_minor", "id_category_secondary", "name", "price", "display_unit",
        "description", "key_information", "rating_1", "rating_2", "rating_3", "rating_4",
        "rating_5", "image_url", "active", "created_at", "modified_at", "modified_by"
    ]]

    return df_product_details


def finalise_product_brand(staff_id_created_at, df_product_brand):
    """
    Adds missing active, created_at, modified_at, modified_by columns to product brand table.

    :param staff_id_created_at: Dictionary containing staff id and created_at date.
    :param df_product_brands: DataFrame containing product brand details. 
    """

    for i, row in df_product_brand.iterrows():
        df_product_brand.at[i, "active"] = "1"

        staff_id, staff_created_at = random.choice(list(staff_id_created_at.items()))
        product_created_at = get_datetime_between(datetime.strptime(staff_created_at, "%Y-%m-%d %H:%M:%S"), datetime.now())

        df_product_brand.at[i, "created_at"] = product_created_at.strftime("%Y-%m-%d %H:%M:%S")
        df_product_brand.at[i, "modified_at"] = product_created_at.strftime(
            "%Y-%m-%d %H:%M:%S") if random.randint(0, 3) <= 2 else get_datetime_between(product_created_at, datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
        df_product_brand.at[i, "modified_by"] = str(staff_id)

    return df_product_brand


def finalise_product_category(df_product_category):
    """
    Adds missing active column to product category table.
    Also drops url column.

    :param df_product_category: DataFrame containing product category details. 
    """

    df_product_category = df_product_category.drop(["url"], axis=1)
    df_product_category.insert(loc=3, column="active", value="1")

    return df_product_category


def get_datetime_between(start_datetime, end_datetime):
    """
    Generate a random datetime object between start_datetime and end_datetime.

    :param start_datetime: The starting datetime.
    :param end_datetime: The ending datetime.
    :return: A random datetime object.
    """

    total_seconds = int((end_datetime - start_datetime).total_seconds())
    random_seconds = random.randint(0, total_seconds)

    return start_datetime + timedelta(seconds=random_seconds)


if __name__ == "__main__":
    main()
