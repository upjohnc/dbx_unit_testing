import sys

import jinja2
import yaml


def main(env="dev"):
    cluster_file = "cluster_dev_template.json"
    if env == "prod":
        cluster_file = "cluster_prod_template.json"
    environment = jinja2.Environment(
        loader=jinja2.FileSystemLoader("dlt_pipeline_configuration")
    )
    template = environment.get_template(cluster_file)

    with open("./dlt_pipeline_configuration/pipeline_template_input.yml") as f:
        input_data = yaml.load(f, Loader=yaml.Loader)

    with open("./dlt_pipeline_configuration/pipeline_temp_config.json", "w") as f:
        f.write(template.render(input_data[env]))


if __name__ == "__main__":
    environment = "dev"
    if len(sys.argv) > 1:
        environment = sys.argv[1]
    main(environment)
