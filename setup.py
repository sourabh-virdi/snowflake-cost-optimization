"""
Setup configuration for Snowflake Cost Optimizer.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text(encoding="utf-8") if readme_file.exists() else ""

# Read requirements
requirements_file = Path(__file__).parent / "requirements.txt"
if requirements_file.exists():
    with open(requirements_file, 'r') as f:
        requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]
else:
    requirements = [
        "snowflake-snowpark-python>=1.11.1",
        "snowflake-connector-python>=3.6.0",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "streamlit>=1.28.0",
        "plotly>=5.17.0",
        "pyyaml>=6.0",
        "python-dotenv>=1.0.0",
        "pydantic>=2.5.0",
        "scikit-learn>=1.3.0",
        "loguru>=0.7.0",
    ]

setup(
    name="snowflake-cost-optimizer",
    version="1.0.0",
    author="Snowflake Optimizer Team",
    author_email="team@snowflake-optimizer.com",
    description="Intelligent Data Governance & Cost Optimization for Snowflake",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/sourabh-virdi/snowflake-cost-optimization",
    project_urls={
        "Bug Tracker": "https://github.com/sourabh-virdi/snowflake-cost-optimization/issues",
        "Documentation": "https://github.com/sourabh-virdi/snowflake-cost-optimization/docs",
        "Source Code": "https://github.com/sourabh-virdi/snowflake-cost-optimization",
    },
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators", 
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Monitoring",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.7.0",
            "pre-commit>=3.0.0",
        ],
        "docs": [
            "sphinx>=6.0.0",
            "sphinx-rtd-theme>=1.3.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "snowflake-optimizer=snowflake_optimizer.cli:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
    keywords=[
        "snowflake", "cost-optimization", "data-governance", 
        "warehouse-optimization", "analytics", "monitoring"
    ],
) 