try:
   from setuptools import setup;
except ImportError:
   from distutils.core import setup;

version = '0.9';

setup(
    name             = 'dz-oracle-storage'
   ,version          = version
   ,url              = 'https://github.com/pauldzy/dz-oracle-storage'
   ,author_email     = 'paul@dziemiela.com'
   ,license          = 'CC0 1.0 Universal public domain dedication'
   ,packages         = ['dz_oracle_storage']
   ,package_dir      = {'':'src'}
   ,install_requires = [
       'cx_Oracle'
      ,'sqlite3',
    ]
);
