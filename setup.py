from setuptools import setup

version = '0.0.5'

setup(name="nosqlite",
      version=version,
      description='A wrapper for sqlite3 to have schemaless, document-store features',
      classifiers=['Development Status :: 3 - Alpha',
                   'License :: OSI Approved :: MIT License',
                   'Operating System :: OS Independent',
                   'Programming Language :: Python :: 3',
                   'Topic :: Software Development :: Libraries :: Python Modules'],
      keywords='nosql sqlite nosqlite',
      author='Chaiwat Suttipongsakul',
      author_email='cwt@bashell.com',
      url='https://hg.sr.ht/~cwt/nosqlite',
      license='MIT',
      py_modules=['nosqlite'],
      include_package_data=True,
)
