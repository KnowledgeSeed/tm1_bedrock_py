from pathlib import Path
from setuptools import find_packages, setup

PACKAGE_ROOT = Path(__file__).resolve().parent
README_PATH = PACKAGE_ROOT / "README.md"

if README_PATH.exists():
    long_description = README_PATH.read_text(encoding="utf-8")
    long_description_content_type = "text/markdown"
else:
    long_description = "Utilities for exporting and comparing TM1 models in Git-friendly formats."
    long_description_content_type = "text/plain"

setup(
    name="tm1gitpy",
    version="0.1.0",
    description="Utilities for exporting and comparing TM1 models for Git workflows.",
    long_description=long_description,
    long_description_content_type=long_description_content_type,
    author="",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "tm1_git_py": ["*.txt", "*.json"],
    },
    install_requires=[
        "TM1py>=2.1,<3.0",
        "requests>=2.25",
        "tm1_bedrock_py>=0.4.1",
    ],
    python_requires=">=3.8",
)
