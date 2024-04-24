default:
    just --list

# install dev packages
poetry-install:
    poetry install --with dev --no-root

# set up pre-commit the  first time
pre-commit:
    poetry run pre-commit install

pipeline-from-template environment='dev':
    poetry run python ./dlt_pipeline_configuration/pipeline_template.py {{ environment }}

# Create the unit test pipeline
pipeline-dev-create: (pipeline-from-template 'dev')
    databricks pipelines create --json @dlt_pipeline_configuration/pipeline_temp_config.json

# Update the unit test pipeline
pipeline-dev-update pipeline_id: (pipeline-from-template 'dev')
    databricks pipelines update {{ pipeline_id }} --json @dlt_pipeline_configuration/pipeline_temp_config.json

# Create the prod pipeline
pipeline-prod-create: (pipeline-from-template 'prod')
    databricks pipelines create --json @dlt_pipeline_configuration/pipeline_temp_config.json

# Update the prod pipeline
pipeline-prod-update pipeline_id: (pipeline-from-template 'prod')
    databricks pipelines update {{ pipeline_id }} --json @dlt_pipeline_configuration/pipeline_temp_config.json
