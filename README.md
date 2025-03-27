# ProfitScan

A Streamlit-based financial analysis tool for calculating product profitability by allocating expenses across transactions.

## Installation

```bash
pip install streamlit pandas numpy openpyxl
```

## Running the Application

```bash
streamlit run app.py
```

## Required Files

Upload the following CSV or Excel files:

1. **Customers file** - Must include:

   - `Customer` column

2. **Invoices file** - Must include:

   - `Invoice_No`, `Customer`, `Product`, `Quantity`, `Sales_Amount` columns

3. **Products file** - Must include:

   - `Product` column
   - Either `Product_Cost` or `Cost_%` column

4. **Expenses file** - Must include:
   - `Expense`, `Amount` columns
   - Optional pre-filled allocation columns: `Allocate_To`, `Allocate_By_Tran`, `Allocate_By_Value`
