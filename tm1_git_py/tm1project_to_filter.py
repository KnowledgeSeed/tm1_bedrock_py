import json
import re

def parse_ignore_entry(entry: str) -> str:
    tm1_object_pattern = re.compile(r"(\w+)\('([^']*)'\)")
    match = tm1_object_pattern.match(entry)

    if match:
        obj_type = match.group(1).lower()
        obj_name = match.group(2)
        
        rule = f"-/{obj_type}/{obj_name}*"
    else:
        rule = f"-/{entry}*"

    return rule.replace('\\', '/')

def convert_json_to_filter_txt(json_path: str, output_path: str):
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"nincs ilyen file '{json_path}'")
        return
    except json.JSONDecodeError:
        print(f"'{json_path}' fájl formátuma nem megfelelő.")
        return

    output_rules = []

    include_files = data.get("Files", [])
    for item in include_files:
        rule = item if item.startswith('+') else '+' + item
        output_rules.append(rule)

    ignore_entries = data.get("Ignore", [])
    for entry in ignore_entries:
        rule = parse_ignore_entry(entry)
        output_rules.append(rule)

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            for rule in output_rules:
                f.write(rule + "\n")
    except IOError:
        print(f"'{output_path}'")

# if __name__ == "__main__":
#     input_json_file = 'tm1project.json'
#     output_filter_file = 'tm1project_filter.txt'
    
#     convert_json_to_filter_txt(input_json_file, output_filter_file)