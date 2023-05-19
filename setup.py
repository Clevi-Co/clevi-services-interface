from setuptools import setup

setup(
    name='db_interface',
    version='0.0.1',
    description='My private package from private github repo',
    url='https://github.com/Esposito-Ettore-0302/clevi-pip',
    author='Clevi Co',
    author_email='',
    license='unlicense',
    packages=['db_interface'],
    install_requires=['pymongo', 'python-dotenv', 'bson', 'pandas', 'scrapy'],
    zip_safe=False
)