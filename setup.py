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
    readme = "README.md",
    packages=["TM1_bedrock_py"],
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
        "pandas>=2.2.3, <3.0.0",
        "json_logging>=1.3.0, <2.0.0",
        "numpy>=2.2.3, <2.0.0",
        "sqlalchemy>=1.4.36, <3.0.0"
    ],
    extras_require={"dev": [
        "parametrize_from_file>=0.20.0,<1.0.0",
        "pytest>=8.3.4,<9.0.0",
        "apache_airflow>=2.10.5,<3.0.0",
        "build>=1.2.2.post1,<2.0.0",
        "airflow_provider_tm1>=0.3.0,<1.0.0"
        "matplotlib>=3.10.1,<4.0.0",
    ]},
    python_requires=">=3.7"
)
