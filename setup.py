from setuptools import setup

setup(
    name='dhs_scraper',
    version='0.2.0',    
    description='A Scraper for the Historical Dictionary of Switzerland (DHS)',
    url='https://github.com/dddpt/dhs-scraper',
    author='Didier Dupertuis',
    license='Apache License 2.0',
    packages=['dhs_scraper'],
    install_requires=[
        'requests>=2.22.0',
        'lxml>=4.5.0',
        'pandas>=1.3.3'
    ],
    setup_requires=['wheel'],
    classifiers=[
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9'
    ],
)
