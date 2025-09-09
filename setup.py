from setuptools import setup, find_packages

setup(
    name="azure-eventhub-delta-pipeline",
    version="1.0.0",
    description="Azure Event Hub to Delta Lake data ingestion pipeline",
    author="Data Engineering Team",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.5.0",
        "delta-spark>=3.0.0",
        "azure-eventhub>=5.11.0",
        "azure-identity>=1.15.0",
        "azure-storage-file-datalake>=12.14.0",
        "azure-keyvault-secrets>=4.7.0",
        "azure-monitor-opentelemetry>=1.2.0",
        "structlog>=23.2.0",
        "prometheus-client>=0.19.0",
        "pydantic>=2.5.0",
        "pydantic-settings>=2.1.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.12.0",
            "flake8>=6.1.0",
            "mypy>=1.8.0",
        ]
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
