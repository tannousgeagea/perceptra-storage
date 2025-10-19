"""
Setup configuration for perceptra-storage package.
"""
from setuptools import setup, find_packages
from pathlib import Path

# Read README
readme_file = Path(__file__).parent / 'README.md'
long_description = readme_file.read_text() if readme_file.exists() else ''

# Read requirements
requirements_file = Path(__file__).parent / 'requirements.txt'
requirements = []
if requirements_file.exists():
    requirements = [
        line.strip() 
        for line in requirements_file.read_text().splitlines() 
        if line.strip() and not line.startswith('#')
    ]

setup(
    name='perceptra-storage',
    version='0.1.0',
    author='Perceptra Team',
    author_email='dev@perceptra.ai',
    description='Unified storage adapter interface for multi-cloud storage backends',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/perceptra/perceptra-storage',
    packages=find_packages(exclude=['tests', 'tests.*']),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Filesystems',
    ],
    python_requires='>=3.9',
    install_requires=[
        # Core dependencies (no cloud providers by default)
    ],
    extras_require={
        's3': ['boto3>=1.26.0'],
        'azure': ['azure-storage-blob>=12.0.0'],
        'minio': ['minio>=7.1.0'],
        'all': [
            'boto3>=1.26.0',
            'azure-storage-blob>=12.0.0',
            'minio>=7.1.0',
        ],
        'dev': [
            'pytest>=7.0.0',
            'pytest-cov>=4.0.0',
            'pytest-mock>=3.10.0',
            'black>=23.0.0',
            'isort>=5.12.0',
            'flake8>=6.0.0',
            'mypy>=1.0.0',
        ],
    },
    package_data={
        'perceptra_storage': ['py.typed'],
    },
    zip_safe=False,
    keywords='storage cloud s3 azure minio blob filesystem adapter',
    project_urls={
        'Documentation': 'https://docs.perceptra.ai/storage',
        'Source': 'https://github.com/perceptra/perceptra-storage',
        'Tracker': 'https://github.com/perceptra/perceptra-storage/issues',
    },
)