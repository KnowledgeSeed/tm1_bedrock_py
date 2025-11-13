from setuptools import setup
import os

def read_version():
    version_file = os.path.join(os.path.dirname(__file__), 'TM1_bedrock_py', '__init__.py')
    with open(version_file, 'r') as f:
        for line in f:
            if line.startswith('__version__'):
                delim = '"' if '"' in line else "'"
                return line.split(delim)[1]
    raise RuntimeError("Unable to find version string.")

setup(
    name="tm1_bedrock_py",
    version=read_version(),
    description="A python modul for TM1 Bedrock.",
    packages=["TM1_bedrock_py", "TM1_bedrock_py.*"],
    include_package_data=True,
    package_data={"TM1_bedrock_py": ["logging.json"]},
    author="",
    author_email="",
    url="",
    keywords=["TM1", "IBM Cognos TM1", "Planning Analytics", "PA", "Cognos"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "TM1py>=2.1, <3.0",
        "pandas>=2.0.0,<2.2.0",
        "json_logging>=1.3.0, <2.0.0",
        "sqlalchemy>=1.4.42,<2.0.0",
        "pyodbc>=5.2.0,<6.0.0"
    ],
    extras_require={
        "airflow": [
            "airflow_provider_tm1>=0.5.1",
            "apache_airflow>=2.4.0,<=2.11.0"
        ],
        "postgres": [
            "apache-airflow-providers-postgres>=3.3.0,<7.0.0"
        ],
        "microsoft-mssql": [
            "apache-airflow-providers-microsoft-mssql>=3.3.0,<7.0.0"
        ],
        "dev": [
            "parametrize_from_file>=0.20.0,<1.0.0",
            "pytest>=8.3.4,<9.0.0",
            "build>=1.2.2.post1,<2.0.0",
            "matplotlib>=3.10.1,<4.0.0"
        ]
    },
    python_requires=">=3.8"
)
