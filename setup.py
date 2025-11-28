"""
Setup script for ingestion-worker
"""
from setuptools import setup, find_packages

setup(
    name="ingestion-worker",
    version="0.1.0",
    description="Video ingestion worker for AI learning platform",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.10",
    install_requires=[
        "google-cloud-storage>=2.10.0",
        "google-cloud-videointelligence>=2.11.0",
        "google-cloud-aiplatform>=1.60.0",
        "google-cloud-video-transcoder>=1.10.0",
        "asyncpg>=0.29.0",
        "aiohttp>=3.9.0",
        "tenacity>=8.2.0",
        "fastapi>=0.66.0",
        "uvicorn>=0.24.0",
        "python-dotenv>=1.0",
        "flask>=3.0",
        "gunicorn>=21.2",
        "polars>=0.19.0",
        "scikit-learn>=1.3.0",
        "textstat>=0.7.0",
        "nltk>=3.8.0",
        "connectorx>=0.3.0",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "matplotlib>=3.7.0",
        "seaborn>=0.12.0",
    ],
)