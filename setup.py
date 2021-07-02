from setuptools import setup, find_packages
import pathlib

HERE = pathlib.Path(__file__).parent
README = (HERE / 'README.md').read_text()
setup(
    name="pymilvusdm",
    version="2.0",
    author="ZILLIZ",
    packages=find_packages(),
    url='https://github.com/milvus-io/milvus-tools',
    license="Apache-2.0",
    description="Milvus data migration tool",
    long_description=README,
    long_description_content_type='text/markdown',
    install_requires=[
        'PyYAML',
        'uuid',
        'h5py',
        'pymysql',
        'tqdm',
        'pymilvus_orm==2.0.0rc1'
    ],
    classifiers=[
        'Programming Language :: Python :: 3.8',
    ],
    entry_points={"console_scripts": ["milvusdm=pymilvusdm:main"]},
)
