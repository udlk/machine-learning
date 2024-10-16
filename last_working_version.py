import re
import pandas as pd

def extract_tables_and_columns(sql):
    tables = {}

    # Split the SQL into individual statements
    statements = sql.strip().split(';')

    for statement in statements:
        statement = statement.strip()
        if not statement:
            continue

        print(f"Parsing statement: {statement}")  # Debug: current SQL statement

        # Extract columns from the SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', statement, re.IGNORECASE)
        if select_match:
            columns_part = select_match.group(1).strip()
            columns = [col.strip() for col in re.split(r',\s*', columns_part)]
            print(f"Found columns part: {columns}")  # Debug: extracted columns part

            # Extract tables from FROM and JOIN clauses
            tables_found = []
            alias_mapping = {}  # Dictionary to hold alias mappings

            # Extract FROM and JOIN tables
            from_part = re.search(r'FROM\s+(.*?)(\s+WHERE|\s+JOIN|$)', statement, re.IGNORECASE)
            if from_part:
                from_tables = from_part.group(1).strip()
                print(f"Found FROM tables: {from_tables}")  # Debug: found from tables

                for table in re.split(r',\s*|\s+JOIN\s+', from_tables):
                    table = table.strip()
                    schema = 'N/A'
                    if '.' in table:
                        schema, table_name = table.split('.', 1)
                    else:
                        table_name = table

                    alias = None
                    if ' ' in table_name:
                        table_name_parts = table_name.split()
                        table_name = table_name_parts[0]
                        alias = table_name_parts[1] if len(table_name_parts) > 1 else None

                    # Initialize the table if not present
                    if table_name not in tables:
                        tables[table_name] = {'schema': schema, 'columns': set()}  # Use set to prevent duplicates

                    tables_found.append((table_name, alias))
                    if alias:
                        alias_mapping[alias] = table_name

            # Extract JOIN tables
            join_tables = re.findall(r'JOIN\s+([^\s]+)\s*(AS\s+)?([a-zA-Z0-9_]+)?', statement, re.IGNORECASE)
            for join in join_tables:
                join_table = join[0].strip()
                join_alias = join[2].strip() if join[2] else None
                schema = 'N/A'
                if '.' in join_table:
                    schema, join_table = join_table.split('.', 1)

                if join_table not in tables:
                    tables[join_table] = {'schema': schema, 'columns': set()}

                tables_found.append((join_table, join_alias))
                if join_alias:
                    alias_mapping[join_alias] = join_table

                print(f"Found JOIN table: {join_table} with alias: {join_alias}")  # Debug: found join table

            # Map columns from the SELECT clause
            for col in columns:
                matched = False
                if '.' in col:
                    prefix, column_name = col.split('.', 1)
                    if prefix in alias_mapping:
                        table_name = alias_mapping[prefix]
                        tables[table_name]['columns'].add(col)
                        print(f"Mapping column {col} to table {table_name} using alias {prefix}")  # Debug
                        matched = True
                    elif prefix in [t[0] for t in tables_found]:
                        tables[prefix]['columns'].add(col)
                        print(f"Mapping column {col} to table {prefix}")  # Debug
                        matched = True

                if not matched:
                    # Add unqualified column to the last table found
                    if tables_found:
                        last_table_name = tables_found[-1][0]
                        tables[last_table_name]['columns'].add(col)
                        print(f"Adding unqualified column {col} to {last_table_name}")  # Debug

            # Extract ON conditions and map columns accordingly
            on_conditions = re.findall(r'ON\s+(.*?)\s*(?:WHERE|JOIN|$)', statement, re.IGNORECASE)
            for on_condition in on_conditions:
                conditions = [cond.strip() for cond in re.split(r'AND', on_condition)]
                for condition in conditions:
                    if '=' in condition:
                        left_column, right_column = condition.split('=')
                        left_column, right_column = left_column.strip(), right_column.strip()

                        def map_column_to_table(column):
                            if '.' in column:
                                prefix, _ = column.split('.', 1)
                                if prefix in alias_mapping:
                                    table_name = alias_mapping[prefix]
                                    tables[table_name]['columns'].add(column)
                                    print(f"Mapping column {column} to table {table_name} using alias {prefix}")  # Debug
                                elif prefix in [t[0] for t in tables_found]:
                                    tables[prefix]['columns'].add(column)
                                    print(f"Mapping column {column} to table {prefix}")  # Debug

                        map_column_to_table(left_column)
                        map_column_to_table(right_column)

    return tables

def parse_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql = file.read()

    print("Parsing SQL file...")
    tables = extract_tables_and_columns(sql)

    result_data = []
    for table, data in tables.items():
        schema = data['schema']
        for col in data['columns']:
            # Adjusting the column names for the output
            result_data.append([schema, table, col])

    # Create a DataFrame and save it to Excel
    if result_data:
        df = pd.DataFrame(result_data, columns=['Schema', 'Table', 'Column'])
        output_file = 'output.xlsx'
        df.to_excel(output_file, index=False)
        print(f"Data written to {output_file}")
    else:
        print("No data extracted to write to Excel.")

    print("Processing completed.")

if __name__ == '__main__':
    # Path to your SQL file
    sql_file_path = 'input/sqlfile.sql'  # Adjust path if necessary
    parse_sql_file(sql_file_path)

