import defs
import pd_reorder
import random
import decimal
import hashlib
import names
import randomname
import time
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

EMAIL_SUFFIXES = [
    "@gmail.com",
    "@outlook.com", "@hotmail.com", "@live.com", "@msn.com",
    "@yahoo.com", "@ymail.com", "@rocketmail.com",
    "@icloud.com", "@me.com", "@mac.com",
    "@protonmail.com"
]


def main(CUSTOMERS_TO_SPAWN):
    """
    Generates a CSV file containing customer details sorted by created_at date.

    The file contains the following columns:
    - id: id
    - first_name: random first name generated using the names library
    - last_name: random last name generated using the names library
    - email: first_name + last_name + random 4-digit number + random email suffix
    - password: random string from randomname library + "-" + random 3-digit number
    - dob: random date between 80 and 18 years ago
    - telephone: random 8-digit number, starting with 8
    - member_card_sn: 50% chance of being a random 16-digit number, 50% chance of being "null"
    - points: 0.00
    - created_at: random date between 2024-01-01 and now
    - modified_at: 75% chance of being equal to created_at, 25% chance of being a random date between created_at and now
    """

    time_2024_start_epoch = 1704067200
    time_now_epoch = int(time.time())
    eighteen_years_ago = datetime.today() - timedelta(days=18*365.25)  # accounts for leap years
    eighty_years_ago = datetime.today() - timedelta(days=80*365.25)  # accounts for leap years

    with open(defs.FILE_CUSTOMER_DETAILS_UNORDERED, "w") as f:
        f.write("id,first_name,last_name,email,password,dob,telephone,member_card_sn,points,created_at,modified_at\n")

        for i in range(CUSTOMERS_TO_SPAWN):
            first_name = names.get_first_name()
            last_name = names.get_last_name()
            email = f"{first_name.lower()}{last_name.lower()}{"{:04}".format(random.randint(0, 9999))}{random.choice(EMAIL_SUFFIXES)}"
            password = hashlib.sha256(f"{randomname.get_name()}-{random.randint(0, 999)}".encode("utf-8")).hexdigest()
            dob = (eighty_years_ago + (eighteen_years_ago - eighty_years_ago) * random.random()).strftime("%Y-%m-%d")
            telephone = random.randint(80000000, 89999999)
            member_card_sn = gen_16_digit()
            points = decimal.Decimal(0).quantize(decimal.Decimal("0.00"))

            created_at_epoch = random.randint(time_2024_start_epoch, time_now_epoch)
            created_at = datetime.fromtimestamp(created_at_epoch).strftime("%Y-%m-%d %H:%M:%S")

            if random.randint(0, 3) < 3:  # 75% chance of having a modified_at date
                modified_at = datetime.fromtimestamp(random.randint(created_at_epoch, time_now_epoch)).strftime("%Y-%m-%d %H:%M:%S")
            else:
                modified_at = created_at

            f.write(f"{i+1},{first_name},{last_name},{email},{password},{dob},{telephone},{member_card_sn},{points},{created_at},{modified_at}\n")

            if (i+1) % 10000 == 0:
                print(f"[{datetime.now()}] Generated {i+1} customers.")

    print(f"[{datetime.now()}] Generated {CUSTOMERS_TO_SPAWN} customers, unsorted.")

    df_customer = pd.read_csv(defs.FILE_CUSTOMER_DETAILS_UNORDERED, keep_default_na=False)
    df_customer_ordered = pd_reorder.sort_customer(df_customer)
    df_customer_ordered.to_csv(defs.FILE_CUSTOMER_DETAILS_ORDERED, index=False)
    Path(defs.FILE_CUSTOMER_DETAILS_UNORDERED).unlink(missing_ok=False)
    print(f"[{datetime.now()}] Sorted customers by created_at.")


def gen_16_digit():
    """
    Generates either a random 16-digit value or a "null" string, with a 50% chance of each.

    :return: A 16-digit value or "null".
    """
    if random.randint(0, 1) == 0:  # 50% chance of having a member card
        return random.randint(1000000000000000, 9999999999999999)
    else:
        return "null"


if __name__ == "__main__":
    main()
