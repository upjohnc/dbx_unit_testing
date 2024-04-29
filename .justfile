default:
    just --list

# install dev packages
poetry-install:
    poetry install --with dev --no-root

# set up pre-commit the  first time
pre-commit:
    poetry run pre-commit install

bundle-deploy:
    databricks bundle deploy -t dev

bundle-destroy:
    databricks bundle destroy -t dev

bundle-run:
    databricks bundle run unit_test -t dev --full-refresh-all
