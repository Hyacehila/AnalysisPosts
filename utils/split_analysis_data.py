import json
import os
import shutil

def split_analysis_data(input_file, output_dir):
    if not os.path.exists(input_file):
        print(f"File not found: {input_file}")
        return

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Split Charts
        charts = data.get('charts', [])
        print(f"Found {len(charts)} charts.")
        for chart in charts:
            chart_id = chart.get('id', 'unknown_chart')
            # Clean filename
            filename = f"chart_{chart_id}.json".replace('/', '_').replace('\\', '_')
            path = os.path.join(output_dir, filename)
            with open(path, 'w', encoding='utf-8') as out:
                json.dump(chart, out, indent=2, ensure_ascii=False)
        
        # Split Tables
        tables = data.get('tables', [])
        print(f"Found {len(tables)} tables.")
        for table in tables:
            table_id = table.get('id', 'unknown_table')
            filename = f"table_{table_id}.json".replace('/', '_').replace('\\', '_')
            path = os.path.join(output_dir, filename)
            with open(path, 'w', encoding='utf-8') as out:
                json.dump(table, out, indent=2, ensure_ascii=False)

        # Split Execution Log
        if 'execution_log' in data:
            print("Found execution log.")
            path = os.path.join(output_dir, "execution_log.json")
            with open(path, 'w', encoding='utf-8') as out:
                json.dump(data['execution_log'], out, indent=2, ensure_ascii=False)
        
        # Split Insights (if any)
        if 'insights' in data:
             print("Found insights.")
             path = os.path.join(output_dir, "insights.json")
             with open(path, 'w', encoding='utf-8') as out:
                 json.dump(data['insights'], out, indent=2, ensure_ascii=False)

        print(f"Successfully split data into {output_dir}")

    except Exception as e:
        print(f"Error splitting file: {e}")

if __name__ == "__main__":
    split_analysis_data('report/analysis_data.json', 'report/split_data')
