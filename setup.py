from setuptools import setup

setup(
    name='services_interface',
    version='1.1.0',
    description='Clevi package for interfaces',
    url='https://github.com/Clevi-Co/clevi-services-interface',
    author='Clevi Co',
    author_email='',
    license='unlicense',
    packages=['db_interface', 'blob_interface'],
    install_requires=['pymongo', 'python-dotenv', 'bson', 'pandas', 'scrapy', 'azure-storage-blob'],
    zip_safe=False
)