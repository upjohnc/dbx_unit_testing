default:
    just --list

# install dev packages
poetry-install:
    poetry install --with dev --no-root

# set up pre-commit the  first time
pre-commit:
    poetry run pre-commit install

pipeline-deploy:
    databricks bundle deploy -t dev

pipeline-run:
    databricks bundle run unit_test_pipeline -t dev --full-refresh-all

pipeline-destroy:
    databricks bundle destroy -t dev

