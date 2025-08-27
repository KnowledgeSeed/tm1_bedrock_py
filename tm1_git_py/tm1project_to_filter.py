#!/usr/bin/env python3
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


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Convert TM1 project JSON to filter text rules"
    )
    parser.add_argument("json", help="Input tm1project.json path")
    parser.add_argument("output", help="Output filter.txt path")
    args = parser.parse_args()

    convert_json_to_filter_txt(args.json, args.output)


if __name__ == "__main__":
    main()
