# Always prefer setuptools over distutils
from setuptools import setup, find_packages

install_requires = [
    "boto3",
    "requests",
    "getpass3",
    "wget",
    "dictdiffer"
]

extras_require = {
    "cli": ["click"],
    "test": [
        "pytest",
        "pytest-mock",
        "flake8",
        "black",
    ],
    "notebook": [
        "protobuf==3.19.5",  # for discreetly -> google-cloud-kms
        "discreetly[aws,gcp]",
        "pyathena",
        "pandas",
        "fastparquet"
    ]
}


setup(
    name="c360-python-client",
    description="A python client for c360 projects.",
    author="TRA",
    author_email="hello@tra.sg",
    version="0.1.0-alpha.12",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=install_requires,
    extras_require=extras_require,
    entry_points={
        "console_scripts": ["c360=c360_client.cli:cli"],
    },
    python_requires=">=3.7",
)
