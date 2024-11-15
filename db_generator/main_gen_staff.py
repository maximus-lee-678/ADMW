import defs
from datetime import datetime, timedelta
from pathlib import Path
import random
import hashlib
import pandas as pd
import names
import randomname

EMAIL_SUFFIXES = [
    "@gmail.com",
    "@outlook.com", "@hotmail.com", "@live.com", "@msn.com",
    "@yahoo.com", "@ymail.com", "@rocketmail.com",
    "@icloud.com", "@me.com", "@mac.com",
    "@protonmail.com"
]


def main():
    """
    Generate a CSV file based on the staff ids in the product category file.

    Run separately from the other scripts.

    The file contains the following columns:
    - id: id
    - first_name: random first name generated using the names library
    - last_name: random last name generated using the names library
    - email: first_name + last_name + random 4-digit number + random email suffix
    - password: random string from randomname library + "-" + random 3-digit number
    - telephone: random 8-digit number, starting with 8
    - created_at: random date between 2024-01-01 and now
    - modified_at: 75% chance of being equal to created_at, 25% chance of being a random date between created_at and now
    """

    df_product_category = pd.read_csv(defs.FILE_PRODUCT_CATEGORY_WRONG_FIELDS, keep_default_na=False)

    Path(defs.FILE_STAFF_DETAILS).unlink(missing_ok=True)

    staff_details = []

    unique_staff_ids = df_product_category["modified_by"].unique()
    unique_staff_ids = [int(id) for id in unique_staff_ids if id not in ("", None, 0)]
    unique_staff_ids = sorted(unique_staff_ids)
    print(f"[{datetime.now()}] Found {len(unique_staff_ids)} unique staff ids.")

    random_create_datetimes = get_random_datetimes(len(unique_staff_ids), datetime(2024, 1, 1))

    for i, staff_id in enumerate(unique_staff_ids):
        first_name = names.get_first_name()
        last_name = names.get_last_name()

        staff_details.append({
            "id": staff_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": f"{first_name.lower()}{last_name.lower()}{"{:04}".format(random.randint(0, 9999))}{random.choice(EMAIL_SUFFIXES)}",
            "password": hashlib.sha256(f"{randomname.get_name()}-{random.randint(0, 999)}".encode("utf-8")).hexdigest(),
            "telephone": random.randint(80000000, 89999999),
            "created_at": random_create_datetimes[i].strftime("%Y-%m-%d %H:%M:%S"),
            "modified_at": random_create_datetimes[i].strftime("%Y-%m-%d %H:%M:%S") if random.randint(0, 3) <= 2 else get_datetime_between(random_create_datetimes[i], datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
        })

    with open(defs.FILE_STAFF_DETAILS, "w") as f:
        f.write("id,first_name,last_name,email,password,telephone,created_at,modified_at\n")

        for staff_detail in staff_details:
            f.write(f"{staff_detail['id']},{staff_detail['first_name']},{staff_detail['last_name']},{staff_detail['email']},{
                    staff_detail['password']},{staff_detail['telephone']},{staff_detail['created_at']},{staff_detail['modified_at']}\n")

    print(f"[{datetime.now()}] Generated {len(unique_staff_ids)} staff members.")


def get_random_datetimes(count, start_datetime):
    """
    Generate a list of random datetime objects between start_datetime and the current time. Dates are sorted in ascending order.

    :param count: Number of random datetime objects to generate.
    :param start_datetime: The starting datetime.
    :return: A list of random datetime objects.
    """

    end_datetime = datetime.now()
    random_datetimes = []

    total_seconds = int((end_datetime - start_datetime).total_seconds())

    for _ in range(count):
        random_seconds = random.randint(0, total_seconds)
        random_datetimes.append(start_datetime + timedelta(seconds=random_seconds))

    random_datetimes = sorted(random_datetimes)

    return random_datetimes


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
