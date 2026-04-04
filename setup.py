from setuptools import setup, find_packages

setup(
    name="etl-pipeline-code",
    version="1.0.0",
    author="gokulram.krishnan",
    url="https://github.com/gokulramkrishnan/ETL-Pipeline-Code",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "pandas>=2.0.0", "numpy>=1.24.0", "openpyxl>=3.1.0",
        "xlsxwriter>=3.1.0", "lxml>=4.9.0", "pyyaml>=6.0",
        "jinja2>=3.1.0", "rich>=13.0.0", "faker>=20.0.0",
    ],
)
