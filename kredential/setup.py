import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="kredential-lib",
    version="0.0.1",
    author="Felix Marquez",
    author_email="felix@kemok.io",
    description="kredential passbolt authentication",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/sampleproject",
    project_urls={
        "Bug Tracker": "https://github.com/pypa/sampleproject/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=[
        'DateTime==4.3',
        'passbolt-python-api==0.1.3',
        'pymssql==2.1.5',
        'python-gnupg==0.4.5',
        'pytz==2021.1',
        'requests==2.22.0',
        'urllib3==1.25.3',
        'zope.interface==5.2.0'
    ]
)
