default:
    just --list

# install dev packages
poetry-install:
    poetry install --with unit_test --no-root

# set up pre-commit the  first time
pre-commit:
    poetry run pre-commit install

bundle-deploy:
    databricks bundle deploy -t dev

bundle-destroy:
    databricks bundle destroy -t dev --auto-approve

bundle-run:
    databricks bundle run unit_test -t dev --full-refresh-all

sql-validations:
    poetry run python src/sql.py

pytest-unittest:
    poetry run pytest -vv
