# Always prefer setuptools over distutils
from setuptools import setup, find_packages

install_requires = [
    "boto3",
    "requests",
    "getpass3",
]

extras_require = {
    "cli": ["click"],
    "test": [
        "pytest",
        "flake8",
        "black",
    ]
}


setup(
    name="c360-python-client",
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    description="A python client for c360 projects.",
    author="TRA",
    author_email="no-reply@tra.sg",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=install_requires,
    extras_require=extras_require,
    entry_points={
        "console_scripts": ["c360=c360_client.cli:cli"],
    },
)
