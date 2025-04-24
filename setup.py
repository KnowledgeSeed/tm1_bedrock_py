from setuptools import setup

setup(
    name="tm1_bedrock_py",
    version="0.4.1",
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
        "TM1py==2.1",
        "pandas==2.2.3",
        "json_logging==1.3.0",
        "numpy==2.2.3",
        "sqlalchemy==2.0.39"
    ],
    extras_require={"dev": [
        "build",
        "pytest",
        "parametrize_from_file",
        "apache_airflow",
        "airflow_provider_tm1",
        "matplotlib",
        "yaml"
    ]},
    python_requires=">=3.7"
)
