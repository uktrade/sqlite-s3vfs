import setuptools


def long_description():
    with open('README.md', 'r') as file:
        return file.read()


setuptools.setup(
    name='sqlite-s3vfs',
    version='0.0.1',
    author='Department for International Trade',
    author_email='sre@digital.trade.gov.uk',
    description='Virtual filesystem for SQLite to read and write to S3',
    long_description=long_description(),
    long_description_content_type='text/markdown',
    url='https://github.com/uktrade/sqlite-s3vfs',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Topic :: Database',
    ],
    python_requires='>=3.6.0',
    py_modules=[
        'sqlite_s3vfs',
    ],
)
