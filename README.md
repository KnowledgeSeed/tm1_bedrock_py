# tm1_bedrock_py
A python package by Knowledgeseed for TM1 Bedrock.

## Requirements
* Python
* TM1py
* mdxpy
* Pandas
* sqlalchemy

## Usage

#### Install without cloning
Download the latest dist/tm1_bedrock_py-<version_number>-py3-none-any.whl and install in your project.
```
pip install dist/tm1_bedrock_py-<version_number>-py3-none-any.whl
```

#### Install with cloning
```
git clone https://github.com/KnowledgeSeed/tm1_bedrock_py.git
pip install -r requirements.txt
```
For creating a python virtual environment see [Development](#development).

#### Example
Create a connection in TM1 with at least the following parameters set:

* address
* user
* password
* port
* ssl

You can check your connection via running `example/check_connectivity.py`. You can configure your connection in `examples/config_example.ini` or if left empty, via user input from the terminal.

## Development
Run the `build` command before publishing a new version of the package.
### Windows
```
python -m venv .env
.\.env\Scripts\activate
pip install -r requirements-dev.txt
python -m build
```

### Linux/macOS
```
virtualenv .env
source .env/bin/activate
pip install -r requirements-dev.txt
python -m build
```
