import json
import os
import shutil
import pandas as pd

def convert_to_csv(input_dir, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for filename in os.listdir(input_dir):
        if not filename.startswith('table_') or not filename.endswith('.json'):
            continue
            
        file_path = os.path.join(input_dir, filename)
        base_name = os.path.splitext(filename)[0]
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = json.load(f)
            
            data = content.get('data')
            if not data:
                print(f"Skipping {filename}: No data field")
                continue
                
            process_node(data, os.path.join(output_dir, base_name))
            
        except Exception as e:
            print(f"Error processing {filename}: {e}")

def process_node(data, output_prefix):
    """
    Recursively process data structures into CSVs.
    """
    # CASE 1: List of Dicts (Standard Table)
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        try:
            df = pd.DataFrame(data)
            output_path = f"{output_prefix}.csv"
            df.to_csv(output_path, index=False, encoding='utf_8_sig')
            print(f"Saved {output_path}")
        except Exception as e:
            print(f"Failed to convert list to DF for {output_prefix}: {e}")
        return

    # CASE 2: Dictionary
    if isinstance(data, dict):
        if not data:
            return

        # Check if it's a "Dict of Dicts" (Row-oriented data)
        # Criteria: values are dicts, and they look similar (share keys)
        values = list(data.values())
        if len(values) > 0 and isinstance(values[0], dict):
            # Check for homogeneity (optional, but good for safety)
            # Let's try to convert to DataFrame directly
            try:
                df = pd.DataFrame.from_dict(data, orient='index')
                # If the index is meaningful (like timestamps), we want it in the CSV
                df.index.name = 'Key'
                output_path = f"{output_prefix}.csv"
                df.to_csv(output_path, encoding='utf_8_sig')
                print(f"Saved {output_path} (from dict indices)")
                return
            except Exception:
                # If conversion fails, fallback to treating as sub-components
                pass

        # CASE 3: Dictionary of Components (e.g., 'nodes': [], 'edges': [])
        # Recursively process each key
        for key, value in data.items():
            new_prefix = f"{output_prefix}_{key}"
            process_node(value, new_prefix)
        return

    # CASE 4: Primitive or List of Primitives
    # Ignore standalone primitives or simple lists for now, as they don't form valid CSV tables easily
    # print(f"Skipping primitive data at {output_prefix}")
    pass

if __name__ == "__main__":
    convert_to_csv('report/split_data_kaifeng', 'report/csv_tables_kaifeng')
