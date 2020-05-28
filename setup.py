from setuptools import setup

setup(
    name='ccm-extensions',
    version='0.1',
    description='Extensions to Cassandra Cluster Manager',
    author='Adam Zegelin',
    author_email='adam@zegelin.com',
    py_modules=['ccm_extensions'],
    install_requires=['ccm', 'cassandra-driver'],
)