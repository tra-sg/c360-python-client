python setup.py sdist bdist_wheel
twine upload \
    --repository-url https://pypi.c360.ai \
    --username ghosalya \
    --password 92f0PMhSDX3njqlblY23 \
    --verbose \
    dist/*
