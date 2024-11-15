import defs
import main_gen_customer
import main_gen_staff
import main_gen_products
from pathlib import Path
from datetime import datetime


def main():
    files_missing = False
    if not Path(defs.FILE_PRODUCT_MISSING_FIELDS).is_file():
        print(f"[{datetime.now()}] {defs.FILE_PRODUCT_MISSING_FIELDS} not found.")
        files_missing = True
    if not Path(defs.FILE_PRODUCT_BRAND_MISSING_FIELDS).is_file():
        print(f"[{datetime.now()}] {defs.FILE_PRODUCT_BRAND_MISSING_FIELDS} not found.")
        files_missing = True
    if not Path(defs.FILE_PRODUCT_CATEGORY_WRONG_FIELDS).is_file():
        print(f"[{datetime.now()}] {defs.FILE_PRODUCT_CATEGORY_WRONG_FIELDS} not found.")
        files_missing = True
    if not Path(defs.FILE_PRODUCT_COUNTRY).is_file():
        print(f"[{datetime.now()}] {defs.FILE_PRODUCT_COUNTRY} not found.")
        files_missing = True
    
    if files_missing:
        print(f"[{datetime.now()}] Please ensure all required input files are present. Exiting.")
        return

    print(f"[{datetime.now()}] All required input files found, starting data generation.")

    main_gen_customer.main()
    main_gen_staff.main()
    main_gen_products.main()

    print(f"[{datetime.now()}] All data generated.")


if __name__ == "__main__":
    main()
