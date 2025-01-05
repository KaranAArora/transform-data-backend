import pandas as pd
import os
import asyncio

# Path for the state codes file
state_file_path = os.path.join(os.getcwd(), 'templates', 'State_Codes.csv')

# Sales Order Master Read (async)
async def so_tmp(so_file_path="../templates/SO Template.csv"):
    # Offload blocking I/O to a background thread
    so_temp_list = await asyncio.to_thread(read_so_template, so_file_path)
    return so_temp_list

def read_so_template(so_file_path):
    # Reading SO CSV Template
    raw_so_template = pd.read_csv(so_file_path)
    # Converting Columns to List
    return raw_so_template.columns.tolist()

# Item Master Read (async)
async def item_data(raw_item_mst_csv):
    try:        
        # Offload blocking I/O task to background thread
        item_master_raw = await asyncio.to_thread(pd.read_csv, raw_item_mst_csv)
        
        # Concatenate Item Code and Name to create 'ITEM' column
        item_master_raw['Full Item Name'] = item_master_raw['Item Code'] + ' ' + item_master_raw['Display Name']

        # Columns to drop
        item_cols_drop = ['Display Name']

        # Drop unnecessary columns and rename 'Internal ID' to 'Item : Internal ID'
        final_item_mst = item_master_raw.drop(columns=item_cols_drop)
        final_item_mst.rename(columns={'Internal ID': 'Item : Internal ID'}, inplace=True)
        
        return final_item_mst

    except FileNotFoundError as e:
        print(e)
        return pd.DataFrame()  # Return an empty DataFrame if file not found
    except pd.errors.EmptyDataError:
        print(f"Error: The file {raw_item_mst_csv} is empty.")
        return pd.DataFrame()
    except pd.errors.ParserError:
        print(f"Error: There was an issue parsing the file {raw_item_mst_csv}.")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return pd.DataFrame()

# State Master Read (async)
async def state_data():
    try:
        # Check if the file exists
        if not os.path.exists(state_file_path):
            raise FileNotFoundError(f"File not found: {state_file_path}")
        
        # Offload blocking I/O to background thread
        state_codes = await asyncio.to_thread(pd.read_csv, state_file_path)
        
        return state_codes

    except FileNotFoundError as e:
        print(e)
        return pd.DataFrame()  # Return an empty DataFrame if file not found
    except pd.errors.EmptyDataError:
        print(f"Error: The file {state_file_path} is empty.")
        return pd.DataFrame()
    except pd.errors.ParserError:
        print(f"Error: There was an issue parsing the file {state_file_path}.")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return pd.DataFrame()

# Customer Master Read (async)
async def cust_data(raw_cust_mst_csv):
    try:
        # Offload blocking I/O task to background thread
        customer_master_raw = await asyncio.to_thread(pd.read_csv, raw_cust_mst_csv)
        
        # Get state codes data
        state_codes = await state_data()
        if state_codes.empty:
            print("State data is empty, unable to proceed with merging.")
            return pd.DataFrame()

        print(f"Customer master Before merge with GST Codes, rows: {len(customer_master_raw)}")
        
        # Merge customer data with state codes
        cust_mst_join = pd.merge(customer_master_raw, state_codes, on='GST_State_Code', how='left')
        
        print(f"Customer master After merge with GST Codes, rows: {len(cust_mst_join)}")

        # Drop unnecessary columns
        cust_cols_drop = ['ID', 'GST_State_Code', 'Name', 'Customer Name']
        final_cust_mst = cust_mst_join.drop(columns=cust_cols_drop)

        # Rename columns as required
        final_cust_mst.rename(columns={'Internal ID': 'Customer : Internal ID'}, inplace=True)
        final_cust_mst.drop_duplicates('Franchisee Code', inplace=True)
        
        return final_cust_mst

    except FileNotFoundError:
        print(f"Error: The file {raw_cust_mst_csv} was not found.")
        return pd.DataFrame()
    except pd.errors.EmptyDataError:
        print(f"Error: The file {raw_cust_mst_csv} is empty.")
        return pd.DataFrame()
    except pd.errors.ParserError:
        print(f"Error: There was an issue parsing the file {raw_cust_mst_csv}.")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return pd.DataFrame()