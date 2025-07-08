import sys
import os
import unittest
import json
import io
from unittest.mock import patch, mock_open

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from tm1project_to_filter import convert_json_to_filter_txt

REAL_JSON_FILE = os.path.join(current_dir, 'tm1project.json')
REAL_FILTER_FILE = os.path.join(current_dir, 'tm1project_filter.txt')

class TestTm1ProjectToFilter(unittest.TestCase):

    def test_convert_json_to_filter_txt_with_mock(self):
        mock_json_data = {
            "Files": ["+/cubes/*", "/dimensions/Product"],
            "Ignore": ["Cube('Price Cube')", "processes/temp_proc.ti"]
        }
        expected_output_lines = [
            "+/cubes/*", "+/dimensions/*", "+/processes/*", "+/chores/*",
            "+/cubes/*", "+/dimensions/Product",
            "-/cube/Price Cube*", "-/processes/temp_proc.ti*"
        ]
        
        m = mock_open(read_data=json.dumps(mock_json_data))
        with patch('builtins.open', m):
            convert_json_to_filter_txt('dummy.json', 'dummy.txt')
            handle = m()
            written_content = "".join(call.args[0] for call in handle.write.call_args_list)
            
            self.assertEqual(
                [line.strip() for line in written_content.strip().splitlines()],
                [line.strip() for line in expected_output_lines]
            )

    @unittest.skipIf(
        not os.path.exists(REAL_JSON_FILE) or not os.path.exists(REAL_FILTER_FILE),
        f"A teszthez szükséges '{os.path.basename(REAL_JSON_FILE)}' vagy '{os.path.basename(REAL_FILTER_FILE)}' nem található."
    )
    def test_real_json_output_matches_real_filter_file(self):
        with open(REAL_JSON_FILE, 'r', encoding='utf-8') as f:
            real_json_content = f.read()

        m = mock_open(read_data=real_json_content)
        with patch('builtins.open', m):
            convert_json_to_filter_txt(REAL_JSON_FILE, 'dummy_output.txt')
            handle = m()
            generated_content = "".join(call.args[0] for call in handle.write.call_args_list)
            
        with open(REAL_FILTER_FILE, 'r', encoding='utf-8') as f:
            expected_content = f.read()

        self.assertListEqual(
            generated_content.strip().splitlines(),
            expected_content.strip().splitlines(),
            f"A '{os.path.basename(REAL_JSON_FILE)}'-ból generált tartalom nem egyezik a '{os.path.basename(REAL_FILTER_FILE)}' tartalmával."
        )

if __name__ == '__main__':
    unittest.main()