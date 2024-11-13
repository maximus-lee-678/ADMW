def generate_insert_orders(tables):
    """
    Generates an order of tables for insertion based on table dependencies.

    :param tables: list of dictionaries containing table metadata. dictionaries must be structured as follows:
    [{"name": str, "references": list[str]}, ...]

    :returns: list[str]
    """

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
