import streamlit as st
import pandas as pd
import numpy as np
import base64
import io  #  for handling I/O operations, such as reading and writing data to files
import sqlite3

st.set_page_config(
    page_title="ProfitScan",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items=None,
)

# SQLite Database Connection:
# if this db does not exist, SQLite creates it automatically.
conn = sqlite3.connect("my_database.db")

# used to execute SQL queries against the database.
c = conn.cursor()

# Pandas Display Options: Set the maximum number of rows and columns to display
pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)

# Constants for Database Tables and Columns; make code cleaner
TABLE_CUSTOMERS = "customers"
TABLE_INVOICES = "invoices"
TABLE_PRODUCTS = "products"
TABLE_EXPENSES = "expenses"
COLUMN_CUSTOMER = "Customer"
COLUMN_PRODUCT = "Product"
COLUMN_INVOICE_NO = "Invoice_No"
COLUMN_QUANTITY = "Quantity"
COLUMN_SALES_AMOUNT = "Sales_Amount"


# Loads data from a CSV file into a DataFrame
def load_csv(file, table_name):
    df = pd.read_csv(
        file,
        thousands=",",
        decimal=".",
        na_values=["NA", "na", "N/A", "n/a", ""],
    )
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except ValueError:
            pass  # column can't be converted to a number, leave as is

    if "Product" in df.columns:
        df["Product"] = df["Product"].str.strip()

    # store the df in a sqlite table
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    return df


# Function to load data from an Excel file
def load_excel(file, table_name):
    df = pd.read_excel(file)

    if "Product" in df.columns:
        df["Product"] = df["Product"].str.strip()

    df.to_sql(table_name, conn, if_exists="replace", index=False)
    return df


# General loader that calls load_csv or load_excel based on file type
def load_data(file, table_name, required_columns):
    try:
        if file is not None and f"{table_name}_data" not in st.session_state:
            try:
                c.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.commit()
            except sqlite3.Error as e:
                st.error(f"An error occurred: {e.args[0]}")

            file_extension = file.name.split(".")[-1]
            if file_extension == "csv":
                df = load_csv(file, table_name)
            elif file_extension in ["xlsx", "xls"]:
                df = load_excel(file, table_name)

            if table_name == "expenses":
                # Check if the user has already filled the allocation columns
                if (
                    "Allocate_To" in df.columns
                    and "Allocate_By_Tran" in df.columns
                    and "Allocate_By_Value" in df.columns
                ):
                    st.session_state["allocation_columns_pre_filled"] = True
                else:
                    st.session_state["allocation_columns_pre_filled"] = False

            if not set(required_columns).issubset(df.columns):
                st.error(
                    f"The following required columns are missing from the Excel file: {set(required_columns) - set(df.columns)}"
                )
                return False
            st.session_state[f"{table_name}_data"] = df

            return True
        else:
            return False
    except Exception as e:
        st.error(f"Error when trying to read the file: {e}")
        return False


# It stores the fetched data in the session state to avoid redundant queries.
def load_data_from_db(table_name):
    if f"{table_name}_data" in st.session_state:
        return st.session_state[f"{table_name}_data"]
    else:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        st.session_state[f"{table_name}_data"] = df
        return df


# merge data from the customers, invoices, and
# products tables into a single df.
def merge_data(customers_table_name, invoices_table_name, products_table_name):
    if "merged_data" in st.session_state:
        return st.session_state["merged_data"]
    else:
        # Get the columns from the tables
        customers_columns = pd.read_sql_query(
            f"PRAGMA table_info({TABLE_CUSTOMERS})", conn
        )["name"].tolist()
        products_columns = pd.read_sql_query(
            f"PRAGMA table_info({TABLE_PRODUCTS})", conn
        )["name"].tolist()

        if COLUMN_CUSTOMER in customers_columns:
            customers_columns.remove(COLUMN_CUSTOMER)
        if COLUMN_PRODUCT in products_columns:
            products_columns.remove(COLUMN_PRODUCT)

        customers_columns_str = ", ".join(
            [f'customers."{col}"' for col in customers_columns]
        )
        products_columns_str = ", ".join(
            [f'products."{col}"' for col in products_columns]
        )

        # Combine all column strings together with additional commas
        all_columns_str = ", ".join(
            filter(None, ["invoices.*", customers_columns_str, products_columns_str])
        )

        query = f"""
        SELECT 
            {all_columns_str}
        FROM {invoices_table_name} AS invoices
        LEFT JOIN {customers_table_name} AS customers
        ON invoices.Customer = customers.Customer
        INNER JOIN {products_table_name} AS products
        ON invoices.Product = products.Product
        """

        df = pd.read_sql_query(query, conn)
        st.session_state["merged_data"] = df
        return df


# calculates the cost amount for each transaction
# based on either the product cost or cost percentage and the quantity sold
def calculate_cost(df):
    try:
        if "Product_Cost" in df.columns:
            df["Cost_Amount"] = df["Quantity"] * df["Product_Cost"]
        elif "Cost_%" in df.columns:
            df["Cost_Amount"] = df["Sales_Amount"] * df["Cost_%"]
        else:
            st.warning(
                "Neither 'Product_Cost' nor 'Cost_%' column exists in the dataframe. The 'Cost_Amount' column will not be calculated."
            )
            df["Cost_Amount"] = 0
    except Exception as e:
        st.error(f"Cost calculation failed. Error: {e}")
    return df


# extract all unique non-numeric column names
def get_all_columns(df_list):
    all_columns = []
    for df in df_list:
        if df is not None:
            non_numeric_cols = df.select_dtypes(exclude="number").columns.tolist()
            non_empty_cols = [
                col
                for col in non_numeric_cols
                if (df[col].dropna() != pd.Timestamp(0)).any()
            ]
            all_columns.extend(non_empty_cols)
    return list(set(all_columns))


# the decorator caches the output to improve performance on subsequent calls with the same inputs.
@st.cache_data
def process_expenses(df, expenses_df):
    # Adds a column for each unique expense in expenses_df to df, initializing them with zeros
    try:
        unique_expenses = expenses_df["Expense"].unique()
        zero_data = pd.DataFrame(0, index=df.index, columns=unique_expenses)
        df = pd.concat([df, zero_data], axis=1)

        for expense_name in unique_expenses:
            expense_rows = expenses_df[expenses_df["Expense"] == expense_name]
            for _, row in expense_rows.iterrows():
                # The allocate_expense function is called for each row related to an expense
                df = allocate_expense(df, row)
        return df
    except Exception as e:
        st.error(f"Expense processing failed. Error: {e}")
    return df


# Allocates amounts from a single expense row to the DataFrame based on defined rules.
def allocate_expense(df, row):
    try:
        expense_name = row["Expense"]  # e.g Marketing
        allocations = row["Allocate_To"].split(";")  # ["Product=P001"]
        by_tran = row["Allocate_By_Tran"] / len(allocations)  # 0.3 / 1 = 0.3
        by_value = row["Allocate_By_Value"] / len(allocations)  # 0.7 / 1 = 0.7
        total_amount = row["Amount"]  # 210
        amount_by_tran = total_amount * by_tran  # 210 * 0.3 = 63
        amount_by_value = total_amount * by_value  # 210 * 0.7 = 147
        for allocation in allocations:
            df = allocate_based_on_rules(
                df, allocation.strip(), amount_by_tran, amount_by_value, expense_name
            )
    except Exception as e:
        st.error(f"Expense allocation failed. Error: {e}")
    return df


# Applies specific allocation rules to distribute an expense amount across transactions.
def allocate_based_on_rules(
    df, allocation, amount_by_tran, amount_by_value, expense_name
):
    try:
        if allocation.lower() == "all":
            if not df.empty:
                df[expense_name] += amount_by_tran / len(df) + amount_by_value * (
                    df["Sales_Amount"] / df["Sales_Amount"].sum()
                )
        else:
            conditions = allocation.split(";")
            temp_df = df.copy()
            for condition in conditions:
                if "=" in condition:
                    key, value = condition.split("=")
                    if key.strip() in df.columns:
                        temp_df = temp_df[temp_df[key.strip()] == value.strip()]
                    else:
                        st.warning(
                            f"Key '{key.strip()}' does not exist in the dataframe, skipping condition"
                        )
            if not temp_df.empty:
                total_sales = (
                    temp_df["Sales_Amount"].sum() or 1
                )  # Avoid division by zero
                df.loc[temp_df.index, expense_name] += amount_by_tran / len(
                    temp_df
                ) + amount_by_value * (
                    df.loc[temp_df.index, "Sales_Amount"] / total_sales
                )
            else:
                st.warning(f"No rows met the condition for {expense_name}")
    except Exception as e:
        st.error(f"Allocation rule application failed. Error: {e}")
    return df


# calculates total expenses and net profit for each transaction
def calculate_totals(df, expenses_df):
    try:
        expense_columns = []

        # Iterate through the expenses and use the weighted column if available
        for expense_name in expenses_df["Expense"].unique():
            weighted_column_name = f"{expense_name}_Weighted"
            if weighted_column_name in df.columns:
                expense_columns.append(weighted_column_name)
            else:
                expense_columns.append(expense_name)

        # Calculate total expenses using the selected columns
        total_expense = df[expense_columns].sum(axis=1)
        df["Total_Expense"] = total_expense.round(2)  # Round to 2 decimal places
        df["Net_Profit"] = (
            df["Sales_Amount"] - df["Total_Expense"] - df["Cost_Amount"]
        ).round(
            2
        )  # Round to 2 decimal places
    except Exception as e:
        st.error(f"Total calculation failed. Error: {e}")
    return df


# append a row at the bottom of the DataFrame, aggregating total values for all numeric column
def append_totals(df):
    try:
        totals = df.select_dtypes(np.number).sum().rename("Total")
        df.index = df.index.astype(str)  # Convert index to string
        df = pd.concat([df, pd.DataFrame(totals).T])
    except Exception as e:
        st.error(f"Appending totals failed. Error: {e}")
    return df


# remove any columns that are entirely empty,
def remove_empty_columns(df):
    df = df.dropna(how="all", axis=1)
    return df


def file_uploader_with_session_state(name, type):
    uploaded_file = st.file_uploader(f"Upload {name} file", type=type)
    file_extension = None
    if uploaded_file is not None:
        file_extension = uploaded_file.name.split(".")[-1]  # Get the file extension
        st.session_state.uploaded_files[name] = uploaded_file
    return st.session_state.uploaded_files.get(name, None), file_extension


# convert a DF into either a CSV string or an Excel binary format, ready for download
def to_csv_xlsx(df, extension):
    if extension == "csv":
        return df.to_csv(index=False)
    elif extension in ["xlsx", "xls"]:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)  # Exclude index in the Excel file
        return output.getvalue()


def create_download_link(df, filename, extension):
    file_data = to_csv_xlsx(df, extension)
    if isinstance(file_data, str):
        file_data = file_data.encode()
    b64 = base64.b64encode(file_data).decode()
    href = f"<a href='data:file/{extension};base64,{b64}' download='{filename}'><input type='button' value='Click to Download {filename}'></a>"
    return href


def check_uploaded_files():
    if "uploaded_files" not in st.session_state:
        st.session_state.uploaded_files = {}

    customers_file, file_extension = file_uploader_with_session_state(
        TABLE_CUSTOMERS, ["csv", "xlsx"]
    )
    invoices_file, _ = file_uploader_with_session_state(TABLE_INVOICES, ["csv", "xlsx"])
    products_file, _ = file_uploader_with_session_state(TABLE_PRODUCTS, ["csv", "xlsx"])
    expenses_file, _ = file_uploader_with_session_state(TABLE_EXPENSES, ["csv", "xlsx"])

    return customers_file, invoices_file, products_file, expenses_file, file_extension


def generate_report(df, allocation_factor, report_type, expenses_df):
    with st.spinner("Generating the report..."):
        # Columns to sum
        sum_columns = [
            "Quantity",
            "Sales_Amount",
            "Cost_Amount",
            "Total_Expense",
            "Net_Profit",
        ]

        if report_type == "Summary":
            if allocation_factor.lower() == "all":
                report_df = df.loc["Total", sum_columns].to_frame().T
                report_df.index = ["Grand Total"]
            else:
                report_df = df.groupby(allocation_factor)[sum_columns].sum()
                report_df.loc["Grand Total"] = report_df.sum()

            # Rename columns
            report_df.rename(
                columns={col: f"Sum of {col}" for col in sum_columns}, inplace=True
            )

        elif report_type == "Detailed":
            if allocation_factor.lower() == "all":
                report_df = df[
                    df.select_dtypes(np.number).columns.intersection(
                        sum_columns + list(expenses_df["Expense"].unique())
                    )
                ].copy()
                report_df = report_df.groupby(df.index).sum()
            else:
                report_df = df.groupby(allocation_factor)[
                    df.select_dtypes(np.number).columns.intersection(
                        sum_columns + list(expenses_df["Expense"].unique())
                    )
                ].sum()

            if "Total" not in report_df.index:
                report_df.loc["Grand Total"] = report_df.sum()

            # Rename columns
            report_df.rename(
                columns={
                    col: f"Sum of {col}" if col in sum_columns else f"Sum of {col}"
                    for col in report_df.columns
                },
                inplace=True,
            )

        # Round the numeric columns to integers
        numeric_columns = report_df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            report_df[col] = report_df[col].round(0).astype(int)

        display_df = report_df.copy()
        if allocation_factor.lower() != "all":
            report_df = report_df.reset_index(drop=False)

    return report_df, display_df


def add_missing_products_to_products_table(products_table_name, missing_products):
    # dataframe with the missing products and a cost of zero
    missing_products_df = pd.DataFrame(
        {
            "Product": list(missing_products),
            "Product_Cost": 0,
        }  # we put 0 here, we could've put np.nan
    )

    # Append the dataframe to the products table
    missing_products_df.to_sql(
        products_table_name, conn, if_exists="append", index=False
    )

    # Update the products data stored in the session state
    products_df = load_data_from_db(products_table_name)
    st.session_state[f"{products_table_name}_data"] = products_df


def apply_expense_weights(df, expenses_df):
    weighted_df = df.copy()
    for i, row in expenses_df.iterrows():
        if (
            "Weights" in row
            and pd.notna(row["Weights"])
            and st.session_state.enabled_expenses[i]
        ):
            weight_info = eval(row["Weights"])
            column_name = weight_info["column_name"]
            weight_mapping = weight_info["weights"]

            # 1. Calculate the Weight for each row based on the mapping
            weighted_df[f"{row['Expense']}_Weight"] = (
                weighted_df[column_name].map(weight_mapping).fillna(0)
            )

            # 2. Calculate xWeight for each row using the specific value from the Expense column
            weighted_df[f"{row['Expense']}_xWeight"] = (
                weighted_df[row["Expense"]] * weighted_df[f"{row['Expense']}_Weight"]
            )

            # 3. Calculate Total xWeight for the entire dataframe
            total_xWeight = weighted_df[f"{row['Expense']}_xWeight"].sum()

            # 4. Calculate Share for each row
            weighted_df[f"{row['Expense']}_Share"] = (
                weighted_df[f"{row['Expense']}_xWeight"] / total_xWeight
                if total_xWeight != 0
                else 0
            )

            difference = 1.0 - weighted_df[f"{row['Expense']}_Share"].sum()
            weighted_df.loc[
                weighted_df[f"{row['Expense']}_Share"].idxmax(),
                f"{row['Expense']}_Share",
            ] += difference

            # 5. Calculate expenseName_weighted column for the expense
            weighted_df[f"{row['Expense']}_Weighted"] = (
                weighted_df[f"{row['Expense']}_Share"] * row["Amount"]
            )

            # Replace original expense column with weighted value
            weighted_df[row["Expense"]] = weighted_df[f"{row['Expense']}_Weighted"]

            weighted_df.drop(columns=[row["Expense"]], inplace=True)

            # Drop other temporary columns used in the calculation
            weighted_df.drop(
                columns=[
                    f"{row['Expense']}_Weight",
                    f"{row['Expense']}_xWeight",
                    f"{row['Expense']}_Share",
                ],
                inplace=True,
            )

    return weighted_df


def main():
    hide_streamlit_style = """
            <style>
            footer {visibility: hidden;}
            </style>
            """
    st.markdown(hide_streamlit_style, unsafe_allow_html=True)
    # st.set_page_config(layout="wide")

    st.markdown(
        """
        <style>
        .container {
            margin-top: 0;
            text-align: center;
        }
        </style>
        <div class="container">
            <h2 style="color:#ADD8E6"> ProfitScan </h2>
            
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.expander("Instructions", expanded=False):
        st.markdown(
            """
            <div style="background-color: #464e56; padding: 30px; border-radius: 5px;">
                <h2 style='color: white;'>Instructions:</h2>
                <ul style='color: white;'>
                    <li>Please upload the required data files in the sidebar. The files must be in CSV or Excel format.</li>
                    <li>Each file should contain specific columns for the application to work properly:</li>
                    <ul>
                        <li><b>Customers file</b>: Must include a 'Customer' column.</li>
                        <li><b>Invoices file</b>: Must include 'Invoice_No', 'Customer', 'Product', 'Quantity', and 'Sales_Amount' columns.</li>
                        <li><b>Products file</b>: Must include a 'Product' column. Either a 'Product_Cost' or 'Cost_%' column is also required.</li>
                        <li><b>Expenses file</b>: Must include 'Expense' and 'Amount' columns.</li>
                    </ul>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.sidebar.markdown(
        """
        <style>
        .sidebar .sidebar-content {
            height: -30px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.markdown(
            "<h1 style='text-align: center; font-size: 24px;'>Upload your files</h1>",
            unsafe_allow_html=True,
        )

        if "uploaded_files" not in st.session_state:
            st.session_state.uploaded_files = {}

        st.subheader("Customer Data")
        customers_file, file_extension = file_uploader_with_session_state(
            "Customers", ["csv", "xlsx"]
        )

        st.subheader("Transaction Data")
        invoices_file, _ = file_uploader_with_session_state("Invoices", ["csv", "xlsx"])
        products_file, _ = file_uploader_with_session_state("Products", ["csv", "xlsx"])

        st.subheader("Expense Data")
        expenses_file, _ = file_uploader_with_session_state("Expenses", ["csv", "xlsx"])

    if customers_file and invoices_file and products_file and expenses_file:
        customers_required_columns = ["Customer"]
        invoices_required_columns = [
            "Invoice_No",
            "Customer",
            "Product",
            "Quantity",
            "Sales_Amount",
        ]
        products_required_columns = ["Product"]

        expenses_required_columns = ["Expense", "Amount"]

        load_data(customers_file, "customers", customers_required_columns)
        load_data(invoices_file, "invoices", invoices_required_columns)
        load_data(products_file, "products", products_required_columns)
        load_data(expenses_file, "expenses", expenses_required_columns)

        invoices_df = load_data_from_db("invoices")

        products_df = load_data_from_db("products")

        invoices_products = set(invoices_df["Product"].unique())
        products_products = set(products_df["Product"].unique())
        mismatch_products = invoices_products - products_products

        # Merging the data and calculating cost
        merged_df = merge_data("customers", "invoices", "products")

        merged_df = calculate_cost(merged_df)

        all_columns = get_all_columns(
            [
                load_data_from_db("customers"),
                load_data_from_db("invoices"),
                load_data_from_db("products"),
            ]
        )
        all_columns = ["All"] + [col for col in all_columns if col.lower() != "date"]

        # Load expenses data into a DataFrame
        expenses_df = load_data_from_db("expenses")

        weights_applied = False

        processed_df = merged_df.copy()

        if "enabled_expenses" not in st.session_state:
            st.session_state.enabled_expenses = [True] * len(expenses_df)

        if "allocation_columns_pre_filled" not in st.session_state:
            st.session_state["allocation_columns_pre_filled"] = False

        if st.session_state["allocation_columns_pre_filled"]:
            st.success(
                "Allocation columns have been pre-filled in the uploaded Expenses file. The settings will be used as-is."
            )
        else:
            for i, row in expenses_df.iterrows():
                amount_rounded = round(row["Amount"], 2)

                st.markdown(
                    f"<h4 style='text-align: left;'>Allocation rules for <span style='color: yellow;'>{row['Expense']}</span>, Amount: <span style='color: yellow;'>{amount_rounded}</span></h4>",
                    unsafe_allow_html=True,
                )

                # Allow user to define multiple allocation rules
                num_allocation_rules = st.number_input(
                    f"Number of allocation rules for {row['Expense']}:",
                    min_value=1,
                    max_value=len(all_columns),
                    value=1,
                    key=f"num_allocation_rules_{i}",
                )

                allocations = []
                for j in range(num_allocation_rules):
                    factor = st.selectbox(
                        f"Select allocation factor for {row['Expense']} rule {j+1}:",
                        all_columns,
                        index=0,
                        key=f"allocation_factor_{i}_{j}",
                    )
                    if factor == "All":
                        allocations.append(factor.lower())
                    else:
                        factor_values = (
                            pd.concat(
                                [
                                    load_data_from_db("customers"),
                                    load_data_from_db("invoices"),
                                    load_data_from_db("products"),
                                ],
                                axis=0,
                            )[factor]
                            .dropna()
                            .unique()
                            .tolist()
                        )
                        factor_value = st.selectbox(
                            f"Select factor value for {row['Expense']} rule {j+1}:",
                            factor_values,
                        )
                        allocations.append(f"{factor}={factor_value}")

                expenses_df.loc[i, "Allocate_To"] = ";".join(allocations)

                allocation_tran = st.slider(
                    f"Select transaction allocation percentage for {row['Expense']}:",
                    min_value=0,
                    max_value=100,
                    value=50,
                    step=25,
                    key=f"allocation_tran_{i}",
                )

                st.write(
                    """
                    <div style='display: flex; justify-content: space-between; margin-top: -3rem; padding: 1rem; background-color: #0e1118; color: gray'>
                        <span style='flex: 1; text-align: left;'>100% By Value</span>
                        <span style='flex: 1; text-align: right;'>100% By Transaction</span>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                expenses_df.loc[i, "Allocate_By_Tran"] = allocation_tran / 100.0
                expenses_df.loc[i, "Allocate_By_Value"] = 1 - (allocation_tran / 100.0)

                apply_weight = st.checkbox(
                    f"Apply weight to {row['Expense']}?", key=f"apply_weight_{i}"
                )

                st.session_state.enabled_expenses[i] = apply_weight

                if apply_weight:
                    weights_applied = True
                    selected_column = st.selectbox(
                        f"Select column for weight application for {row['Expense']}:",
                        processed_df.columns,
                        key=f"weight_column_{i}",
                    )

                    unique_values = processed_df[selected_column].unique()
                    weights = {}

                    for value in unique_values:
                        weights[value] = st.number_input(
                            f"Enter weight for {selected_column} value '{value}':",
                            min_value=0.0,
                            max_value=100.0,
                            value=1.0,
                            step=0.1,
                            key=f"weight_{selected_column}_{value}_{i}",
                        )

                    weight_info = {"column_name": selected_column, "weights": weights}
                    expenses_df.at[i, "Weights"] = str(weight_info)
                else:
                    expenses_df.at[i, "Weights"] = np.nan

        processed_df = process_expenses(merged_df, expenses_df)

        weighted_final_df = apply_expense_weights(processed_df, expenses_df)

        st.markdown(
            "<h3 style='text-align: left; margin-top: 2rem'>Updated Expenses Table</h3>",
            unsafe_allow_html=True,
        )

        any_weights_applied = any(st.session_state.enabled_expenses)

        expenses_to_display = (
            expenses_df
            if any_weights_applied
            else expenses_df.drop(columns="Weights", errors="ignore")
        )
        st.write(expenses_to_display)

        expenses_download_link = create_download_link(
            expenses_to_display,
            "Updated_Expenses_Table." + file_extension,
            file_extension,
        )
        st.markdown(expenses_download_link, unsafe_allow_html=True)

        processed_df = apply_expense_weights(processed_df, expenses_df)

        processed_df = remove_empty_columns(processed_df)

        processed_df = calculate_totals(processed_df, expenses_df)
        processed_df = append_totals(processed_df)

        st.markdown(
            "<h3 style='text-align: left; margin-top: 2rem'>Final Table</h3>",
            unsafe_allow_html=True,
        )
        st.dataframe(processed_df)

        final_table_download_link = create_download_link(
            processed_df, "Final_Table." + file_extension, file_extension
        )
        st.markdown(final_table_download_link, unsafe_allow_html=True)

        st.markdown(
            "<h3 style='text-align: left; margin-top: 2rem'>Generate Reports</h3>",
            unsafe_allow_html=True,
        )

        if not all_columns:
            st.error("No columns available for report generation.")
        else:
            allocation_factor = st.selectbox(
                "Select the allocation factor for the report:",
                [""] + all_columns,
                index=0,
            )
        report_type = st.selectbox(
            "Select the type of report:", ["Summary", "Detailed"]
        )

        if allocation_factor:
            # show the progress while the page is loading
            with st.spinner("Generating report..."):
                report_df, display_df = generate_report(
                    processed_df, allocation_factor, report_type, expenses_df
                )

            # Format display_df for better readability in Streamlit
            numeric_columns = display_df.select_dtypes(include=[np.number]).columns
            for col in numeric_columns:
                display_df[col] = display_df[col].apply(lambda x: f"{x:,}")

            st.markdown(
                f"<h3 style='text-align: left; margin-top: 2rem'>{report_type} Report</h3>",
                unsafe_allow_html=True,
            )
            st.dataframe(display_df)  # Display formatted dataframe in Streamlit

            report_filename = (
                f"{report_type}_Report_{allocation_factor}.{file_extension}"
            )
            report_download_link = create_download_link(
                report_df, report_filename, file_extension
            )  # Use the unformatted report_df for downloading
            st.markdown(report_download_link, unsafe_allow_html=True)

            conn.close()


if __name__ == "__main__":
    main()
