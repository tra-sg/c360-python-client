# Make sure to set up the corresponding environment variable before running:
# - C360_PRIVATE_PYPI_USER
# - C360_PRIVATE_PYPI_PASS

python setup.py sdist bdist_wheel
twine upload \
    --repository-url https://pypi.c360.ai \
    --username $C360_PRIVATE_PYPI_USER \
    --password $C360_PRIVATE_PYPI_PASS \
    --verbose \
    dist/*
