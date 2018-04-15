from setuptools import setup, find_packages

install_requires = [
    'pandas>0.2',
]

setup(
        name = 'backfire',
        version = '0.1',
        scripts = ['backtest.py', 'backtest_turtle.py'],
        author = 'James',
        packages = find_packages(),
        install_requires = install_requires,
        py_modules = ['backfire'],
)
