from pathlib import Path
from datetime import datetime
import defs
import customer
import customer_cart
import customer_transactions

CUSTOMERS_TO_SPAWN = 5242880
# CUSTOMERS_TO_SPAWN = 1048576
# CUSTOMERS_TO_SPAWN = 102400
# CUSTOMERS_TO_SPAWN = 10240


def main():
    print(f"[{datetime.now()}] Generating data for {CUSTOMERS_TO_SPAWN} customers.")

    Path(defs.FILE_CUSTOMER_DETAILS_ORDERED).unlink(missing_ok=True)
    Path(defs.FILE_CUSTOMER_DETAILS_UNORDERED).unlink(missing_ok=True)
    Path(defs.FILE_CUSTOMER_CART_ORDERED).unlink(missing_ok=True)
    Path(defs.FILE_CUSTOMER_CART_RANDOM).unlink(missing_ok=True)
    Path(defs.FILE_CUSTOMER_PURCHASE_INSTANCE_ORDERED).unlink(missing_ok=True)
    Path(defs.FILE_CUSTOMER_PURCHASE_INSTANCE_UNORDERED).unlink(missing_ok=True)
    Path(defs.FILE_CUSTOMER_PURCHASE_INSTANCE_ITEMS_ORDERED).unlink(missing_ok=True)
    Path(defs.FILE_CUSTOMER_PURCHASE_INSTANCE_ITEMS_UNORDERED).unlink(missing_ok=True)
    
    customer.main(CUSTOMERS_TO_SPAWN)
    customer_cart.main(CUSTOMERS_TO_SPAWN)
    customer_transactions.main(CUSTOMERS_TO_SPAWN)

    print(f"[{datetime.now()}] Done.")

if __name__ == "__main__":
    main()
