from setuptools import setup, find_packages

setup(
    name="pista",
    version="1.0.0",
    description="Python Service for Test Automation",
    author="Satya",
    author_email="",
    packages=find_packages(),
    install_requires=[
        "selenium==4.11.2",
        "webdriver-manager==4.0.0",
        "pytest==7.2.0",
        "pytest-order==1.0.1",
        "pytest-dependency==0.5.1",
        "pytest-xdist==3.1.0",
        "pytest-excel==1.5.2",
        "pytest-md-report==0.3.0",
        "cx-Oracle==8.3.0",
        "cryptography==41.0.5",
        "requests==2.28.1",
        "openpyxl==3.1.2",
        "jsonschema==4.19.0",
        "WMI==1.5.1",
        # Add other dependencies as needed
    ],
    extras_require={
        "reporting": [
            "Jinja2==3.1.2",
            "ansi2html==1.8.0",
            "htmlmin==0.1.12",
            "docutils==0.19",
            "pytest_metadata==3.0.0",
        ],
        "api": [
            # Add API-related dependencies here (e.g., jsonpath-ng, pandas, numpy)
            "pandas==2.1.3",
            "numpy==1.26.2",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
