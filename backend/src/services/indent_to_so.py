import pandas as pd
from datetime import datetime
from utils.clean_up import clean_up_file
from flask import jsonify,current_app
import os
from services.read_masters import item_data,cust_data
from dotenv import load_dotenv
from io import StringIO

load_dotenv()

#Masters Variables
raw_item_mst_csv = f"https://drive.google.com/uc?id={os.getenv('SB_RAW_ITEM_MST_CSV_ID')}"
raw_cust_mst_csv = f"https://drive.google.com/uc?id={os.getenv('SB_RAW_CUST_MST_CSV_ID')}"

#raw_item_mst_csv = f"https://drive.google.com/uc?id={os.getenv('PROD_RAW_ITEM_MST_CSV_ID')}"
#raw_cust_mst_csv = f"https://drive.google.com/uc?id={os.getenv('PROD_RAW_CUST_MST_CSV_ID')}""

# Set Current Date and Date Format
date_format = '%d-%m-%Y'
changed_format = '%d/%m/%Y'
current_date = datetime.now().strftime(date_format)

# Creating Indent and Sales Order Header Mapping
SO_IND_MAP = {
    'Supplier' : 'LOCATION', 
    'Receiver' : 'CUSTOMER', 
    'Date' : 'DATE', 
    'Item Code' : 'ITEM CODE', 
    'Item Name' : 'ITEM NAME', 
    'Requested Qty' : 'REQUESTED QUANTITY', 
    'Unit' : 'UNITS', 
    'UnitPrice' : 'UNIT PRICE', 
    'SubTotal' : 'AMOUNT'
}

async def indent_so_process(filepath):
    try:
        # Check if the file exists
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found at: {filepath}")

        # Reading CSV file with only required columns based on so_ind_map
        indent_rawdata = pd.read_csv(filepath, usecols=lambda column: column.strip() in SO_IND_MAP.keys())

        # Renaming columns based on the so_ind_map
        indent_rawdata.rename(columns=SO_IND_MAP, inplace=True)

        # Create 'ITEM' column by concatenating Item Code and Item Name using vectorized operation
        indent_rawdata['Item'] = indent_rawdata['Item Code '] + ' ' + indent_rawdata['Item Name ']

        # Replacing Warehouse Names using vectorized operations for efficiency
        WAREHOUSE_MAP = {
            'Central Warehouse - Mumbai - Bloombay Enterprise Private Limited': 'BEPL Mumbai Warehouse',
            'BWC-NCR-Delhi-WRH': 'BEPL Delhi Warehouse',
            'BWC-KA-Bengaluru-WRH': 'BEPL Bengaluru Warehouse'
        }
        indent_rawdata['Location'] = indent_rawdata['Supplier '].map(WAREHOUSE_MAP).fillna(indent_rawdata['Supplier '])

        # Drop unnecessary columns
        indent_rawdata.drop(columns=['Item Name ', 'Supplier '], inplace=True)

        # Creating Line Level Location
        indent_rawdata['Location Item'] = indent_rawdata['Location']
        indent_rawdata['Quantity'] = indent_rawdata['Requested Qty ']

        # print(indent_rawdata)
        # print(list(indent_rawdata.columns))
        # Renaming columns to match the desired output
        updated_column_names = ['Customer', 'Date','Item Code', 'Units', 'Unit Price', 'Requested Quantity', 'Amount', 'Item', 'Location', 'Location Item', 'Quantity']
        indent_rawdata.columns = updated_column_names

        # Create the dict_indent list for external processing
        dict_indent = indent_rawdata.to_dict(orient='records')

        #print(dict_indent)
        # Creating External ID
        formatted_date = current_date.replace('-', '')
        for item in dict_indent:
            franchisee_code = item['Customer'].split('-')[0].strip().replace('/', '')
            external_id = f'{franchisee_code}{formatted_date}'
            item['External ID'] = external_id

        #Creating Dataframe from Combined Dict 
        final_dict = pd.DataFrame(dict_indent)

        #Replacing Date format in Date Cloumn
        final_dict['Date'] = pd.to_datetime(final_dict['Date'], format=date_format).dt.strftime(changed_format)
        
        final_dict['Franchisee Code'] = final_dict['Customer'].str.split('-').str[0]

        print(f"Before merge with item master, rows: {len(final_dict)}")
        #print(final_dict)

        #Join Item Master and Processed Indent Data
        final_item_mst = await item_data(raw_item_mst_csv)
        try:
            join_item = pd.merge(final_dict, final_item_mst, on='Item Code', how='inner')
        except KeyError as e:
            print(f"Merge failed on ITEM: {e}")
            return []
        #print(join_item)
        print(f"After merge with item master, rows: {len(join_item)}")

        #Adding Customer Master to the Above Joined Data
        final_cust_mst = await cust_data(raw_cust_mst_csv)
        try:
            join_customer = pd.merge(join_item, final_cust_mst, on='Franchisee Code', how='inner')
            print(f"After merge with Customer master, rows: {len(join_customer)}")
        except KeyError as e:
            print(f"Merge failed on CUSTOMER: {e}")
            return []
        
        #print(join_customer)
        processed_data = join_customer.copy()

        current_date_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        # Generate File Name
        processed_file_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"SO_Indent_Dt_{current_date_time}.csv")

        # Save the DataFrame to the file path
        processed_data.to_csv(processed_file_path, index=False)

        return processed_file_path

    except FileNotFoundError as e:
        print(e)
        return []
    except pd.errors.EmptyDataError:
        print(f"Error: The file {filepath} is empty.")
        return []
    except pd.errors.ParserError:
        print(f"Error: There was an issue parsing the file {filepath}")
        return jsonify({"Error": f"There was an issue parsing the file {filepath}. Error details: {str(e)}"}), 400
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []
    finally:
        # Clean up the temporary file after processing
        clean_up_file(filepath)

