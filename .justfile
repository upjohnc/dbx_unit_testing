default:
    just --list

# install dev packages
poetry-install:
    poetry install --with dev --no-root

# set up pre-commit the  first time
pre-commit:
    poetry run pre-commit install

# Create the unit test pipeline
pipeline-dev-create:
    databricks pipelines create --json @dlt_pipeline_configuartion/cluster_test.json

# Update the unit test pipeline
pipeline-dev-update pipeline_id:
    databricks pipelines update {{ pipeline_id }} --json @dlt_pipeline_configuartion/cluster_test.json

# Create the prod pipeline
pipeline-prod-create:
    databricks pipelines create --json @dlt_pipeline_configuartion/cluster_prod.json

# Update the prod pipeline
pipeline-prod-update pipeline_id:
    databricks pipelines update {{ pipeline_id }} --json @dlt_pipeline_configuartion/cluster_prod.json
