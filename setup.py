import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="kemokrw",
    version="0.0.4",
    author="Kevin Mazariegos",
    author_email="kevin@kemok.io",
    description="Paquete de extracción y carga desde diferentes tipo de origenes y destinos",
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
        'pandas>=1.1.5',
        'psycopg2-binary>=2.8.6',
        'pymssql>=2.2.1',
        'SQLAlchemy>=1.3.24',
        'requests>=2.25.1',
        'google-api-python-client>=1.12.8',
        'google-auth-httplib2>=0.1.0',
        'google-auth-oauthlib>=0.4.4',
        'unidecode>=1.2.0'
    ]
)