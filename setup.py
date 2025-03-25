from setuptools import setup

setup(
    name="tm1_bedrock_py",
    version="0.1.0",
    description="A python modul for TM1 Bedrock.",
    readme = "README.md",
    packages=["TM1_bedrock_py"],
    author="",
    author_email="",
    url="",
    keywords=["TM1", "IBM Cognos TM1", "Planning Analytics", "PA", "Cognos"],
    classifiers = [
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "TM1py",
        "mdxpy",
        "pandas",
        "json_logging",
        "sqlalchemy"
    ],
    extras_require={"dev": [
        "build",
        "pytest",
        "parametrize_from_file",
        "apache_airflow",
        "airflow_provider_tm1"
    ]},
    python_requires=">=3.7"
)
