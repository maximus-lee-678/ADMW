def generate_insert_orders(tables):
    def order_tables(dependencies, all_tables):
        ordered_tables = []

        def dfs(table):
            if table in ordered_tables:
                return
            for ref in graph[table]:
                dfs(ref)
            ordered_tables.append(table)

        graph = {}
        for table, ref in dependencies:
            if table not in graph:
                graph[table] = set()
            if ref not in graph:
                graph[ref] = set()
            graph[table].add(ref)

        for table in graph.keys():
            dfs(table)

        for table in all_tables:
            if table not in ordered_tables:
                ordered_tables.append(table)

        return ordered_tables

    constraints = []    # (table_name, depends_on)
    all_tables = []

    for table in tables:
        all_tables.append(table["name"])
        if table["references"]:
            for depends_on in table["references"]:
                constraints.append((table["name"], depends_on))

    order_of_creation = order_tables(dependencies=constraints, all_tables=all_tables)

    return order_of_creation


def main():
    tables = [
        {"name": "PRODUCT_DETAILS", "references": ["PRODUCT_BRAND", "PRODUCT_COUNTRY", "PRODUCT_CATEGORY", "PRODUCT_CATEGORY", "PRODUCT_CATEGORY", "STAFF_DETAILS"]},
        {"name": "PRODUCT_CATEGORY", "references": ["STAFF_DETAILS"]},
        {"name": "PRODUCT_BRAND", "references": ["STAFF_DETAILS"]},
        {"name": "PRODUCT_COUNTRY", "references": []},
        {"name": "PRODUCT_SECONDARY_CATEGORY", "references": ["PRODUCT_DETAILS", "PRODUCT_CATEGORY"]},
        {"name": "CUSTOMER_DETAILS", "references": []},
        {"name": "CUSTOMER_PURCHASE_INSTANCE", "references": ["CUSTOMER_DETAILS"]},
        {"name": "CUSTOMER_PURCHASE_INSTANCE_ITEMS", "references": ["CUSTOMER_PURCHASE_INSTANCE", "PRODUCT_DETAILS"]},
        {"name": "CUSTOMER_CART_ITEMS", "references": ["CUSTOMER_DETAILS", "PRODUCT_DETAILS"]},
        {"name": "STAFF_DETAILS", "references": []}
    ]

    print(" > ".join(generate_insert_orders(tables)))


if __name__ == "__main__":
    main()
