import os
import re
import pandas as pd


def extract_schema_table_column(sql, start_line):
    # Regular expression to find the table from the 'FROM' or 'JOIN' clause (schema.table or just table)
    table_regex = re.compile(
        r"(FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\.\s*([a-zA-Z_][a-zA-Z0-9_]*))?(?:\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*))?",
        re.IGNORECASE)

    # Regular expression to find columns from the 'SELECT' clause
    columns_regex = re.compile(r"SELECT\s+(.*?)\s+(?:FROM|WHERE|GROUP BY|ORDER BY|LIMIT|HAVING)", re.IGNORECASE)

    schema_table_columns = []
    alias_to_table_map = {}

    print(f"Parsing SQL starting at line {start_line}")

    # Extract the schema and table (schema.optional.table)
    table_matches = table_regex.findall(sql)
    tables = []
    for match in table_matches:
        join_type = match[0]
        schema_name = match[2] if match[2] else "N/A"
        table_name = match[1]
        alias = match[3] if match[3] else "N/A"

        # If schema is not provided and the table is in the format schema.table, split it
        if schema_name == "N/A" and '.' in table_name:
            schema_name, table_name = table_name.split('.', 1)

        # Map alias to schema and table
        if alias != "N/A":
            alias_to_table_map[alias] = {"schema": schema_name, "table": table_name}

        tables.append({
            "Join Type": join_type,
            "Schema": schema_name if schema_name != "N/A" else "N/A",
            "Table": table_name if table_name != "N/A" else "N/A",
            "Alias": alias if alias != "N/A" else "N/A"
        })

        # Debugging the capture of table names
        print(f"Join Type: {join_type}, Schema: {schema_name}, Table: {table_name}, Alias: {alias}")

    # Extract columns, excluding any WHERE, GROUP BY, ORDER BY clauses
    column_match = columns_regex.search(sql)
    if column_match:
        # Extract columns (ignore the SQL clause details like WHERE, ORDER BY)
        columns_part = column_match.group(1).strip()

        # Split the columns by commas and clean extra spaces
        columns = [col.strip() for col in columns_part.split(',')]

        # For columns that reference aliases (e.g., e.first_name, d.department_name), map them to the correct table
        for idx, col in enumerate(columns):
            if '.' in col:
                alias, column = col.split('.', 1)
                if alias in alias_to_table_map:
                    schema, table = alias_to_table_map[alias]["schema"], alias_to_table_map[alias]["table"]
                    columns[idx] = f"{schema}.{table}.{column}"

        # Debugging the columns for each table
        print(f"Columns for Tables: {', '.join(columns)}")
    else:
        columns = ["N/A"]

    # Add schema, table, columns to the schema_table_columns list
    for table in tables:
        schema_table_columns.append({
            "Starting Line": start_line,
            "Schema": table['Schema'],
            "Table": table['Table'],
            "Columns": ", ".join(columns) if columns else "N/A"
        })

    return schema_table_columns


def add_static_columns(df, static_data_df):
    # Create an empty list to hold the updated rows
    updated_rows = []

    # Iterate through the rows of the DataFrame
    for index, row in df.iterrows():
        # Get the list of columns (split by commas)
        columns = row['Columns'].split(',')

        # Initialize the static values as "N/A" by default
        sensitivity_level = "N/A"
        critical_data_element = "N/A"
        pii = "N/A"
        financial_data = "N/A"
        health_data = "N/A"
        third_party_data = "N/A"
        regulatory_compliance = "N/A"

        # Check each column against the static data and update the static values
        for column in columns:
            column = column.strip()  # Remove any leading or trailing spaces
            print(f"Checking column: {column} in table: {row['Table']} and schema: {row['Schema']}")

            match = static_data_df[
                (static_data_df['Column Name'].str.lower() == column.lower()) &
                (static_data_df['Schema Name'].str.lower() == row['Schema'].lower()) &
                (static_data_df['Table Name'].str.lower() == row['Table'].lower())
                ]

            if not match.empty:
                # Extract the first match found (assuming one-to-one mapping for simplicity)
                sensitivity_level = match['Sensitivity Level'].values[0]
                critical_data_element = match['Critical Data Element'].values[0]
                pii = match['Personal Identifiable Information'].values[0]
                financial_data = match['Financial Data'].values[0]
                health_data = match['Health Data'].values[0]
                third_party_data = match['3rd Party Data'].values[0]
                regulatory_compliance = match['Regulatory and Compliance Data'].values[0]
                print(f"Match found for column: {column}")
                break  # Since it's a match, we can stop looking further
            else:
                print(f"No match found for column: {column}")

        # Add the updated row with the new static column values
        updated_row = row.copy()
        updated_row['Sensitivity Level'] = sensitivity_level
        updated_row['Critical Data Element'] = critical_data_element
        updated_row['Personal Identifiable Information'] = pii
        updated_row['Financial Data'] = financial_data
        updated_row['Health Data'] = health_data
        updated_row['3rd Party Data'] = third_party_data
        updated_row['Regulatory and Compliance Data'] = regulatory_compliance

        updated_rows.append(updated_row)

    # Create a new DataFrame with the updated rows
    updated_df = pd.DataFrame(updated_rows)

    return updated_df


def process_sql_files(input_folder, output_excel, static_data_df):
    all_schema_table_columns = []

    for file_name in os.listdir(input_folder):
        if file_name.endswith(".sql"):
            file_path = os.path.join(input_folder, file_name)
            with open(file_path, "r", encoding="utf-8") as f:
                sql_content = f.readlines()

            sql_statements = []
            current_statement = []
            for idx, line in enumerate(sql_content, start=1):
                line = line.strip()
                if line.startswith("--") or not line:
                    continue

                if ";" in line:
                    current_statement.append(line)
                    sql_statements.append((idx, " ".join(current_statement)))
                    current_statement = []
                else:
                    current_statement.append(line)

            for start_line, sql in sql_statements:
                try:
                    schema_table_columns = extract_schema_table_column(sql, start_line)
                    all_schema_table_columns.extend(schema_table_columns)
                except Exception as e:
                    print(f"Error processing SQL statement: {sql}\n{e}")

    # Output to DataFrame before adding static columns
    df = pd.DataFrame(all_schema_table_columns)

    # Add static columns based on reference data
    updated_df = add_static_columns(df, static_data_df)

    # Write final output to Excel
    updated_df.to_excel(output_excel, index=False)
    print(f"Output written to {output_excel}")


if __name__ == "__main__":
    input_folder = "input"  # Folder containing the SQL files
    output_excel = "output.xlsx"  # Name of the Excel file to store the output

    # Sample static data for matching with SQL columns
    static_data = {
        'Table Name': ['employees', 'departments', 'employees', 'employees'],
        'Schema Name': ['N/A', 'N/A', 'N/A', 'N/A'],
        'Column Name': ['e.employee_id', 'd.department_id', 'e.salary', 'e.first_name'],
        'Sensitivity Level': ['High', 'Low', 'High', 'Low'],
        'Critical Data Element': ['Y', 'N', 'Y', 'N'],
        'Personal Identifiable Information': ['Y', 'N', 'Y', 'N'],
        'Financial Data': ['Y', 'N', 'Y', 'N'],
        'Health Data': ['N', 'N', 'N', 'N'],
        '3rd Party Data': ['Y', 'N', 'N', 'N'],
        'Regulatory and Compliance Data': ['Y', 'N', 'Y', 'Y']
    }

    static_data_df = pd.DataFrame(static_data)

    process_sql_files(input_folder, output_excel, static_data_df)
