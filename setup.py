from setuptools import setup, find_packages
import pathlib

HERE = pathlib.Path(__file__).parent
README = (HERE / 'README.md').read_text()
setup(
    name="pymilvusdm",
    version="0.1.1-2",
    author="ZILLIZ",
    packages=find_packages(),
    url='https://github.com/milvus-io/milvus-tools',
    license="Apache-2.0",
    description="Milvus data migration tool",
    long_description=README,
    long_description_content_type='text/markdown',
    install_requires=[
        'PyYAML>=5.3.1',
        'uuid>=1.3.0',
        'h5py>=3.1.0',
        'pymysql',
        'tqdm>=4.56.0',
        'pymilvus'
    ],
    classifiers=[
        'Programming Language :: Python :: 3.8',
    ],
    entry_points={"console_scripts": ["milvusdm=pymilvusdm:main"]},
)
