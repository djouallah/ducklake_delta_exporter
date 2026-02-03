# File: setup.py
from setuptools import setup, find_packages

setup(
    name='ducklake-delta-exporter',
    version='0.3.0',
    packages=find_packages(),
    install_requires=['duckdb>=1.4.4'],
    author='mim',
    author_email='your.email@example.com',
    description='A utility to export DuckLake database metadata to Delta Lake transaction logs.',
    long_description=open('README.md', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/djouallah/ducklake_delta_exporter',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Development Status :: 3 - Alpha',
    ],
    python_requires='>=3.8',
)