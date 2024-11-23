import os
from datetime import datetime
import nbformat
from nbconvert import HTMLExporter, NotebookExporter
from nbconvert.preprocessors import ExecutePreprocessor
from dagster import job, op, schedule, Definitions, get_dagster_logger


logger = get_dagster_logger()

# Path to the Jupyter notebook
NOTEBOOK_PATH = r"E:\Repository\web-scraping-project\notebook.ipynb"
OUTPUT_NOTEBOOK_PATH = NOTEBOOK_PATH
OUTPUT_HTML_PATH = r"E:\Repository\web-scraping-project\notebook_output.html"


@op
def run_jupyter_notebook():
    """
    Run a Jupyter notebook using nbconvert and save the executed notebook as HTML.
    The output of the notebook will be saved as 'notebook_output.ipynb' and 'notebook_output.html'.
    """
    try:
        # Read the notebook
        with open(NOTEBOOK_PATH) as f:
            notebook = nbformat.read(f, as_version=4)

        # Execute the notebook
        execute_preprocessor = ExecutePreprocessor(timeout=600, kernel_name="python3")
        execute_preprocessor.preprocess(
            notebook, {"metadata": {"path": os.path.dirname(NOTEBOOK_PATH)}}
        )

        # Save the executed notebook to the output path
        with open(OUTPUT_NOTEBOOK_PATH, "w", encoding="utf-8") as f:
            nbformat.write(notebook, f)
        logger.info(
            f"Notebook executed successfully and saved to {OUTPUT_NOTEBOOK_PATH}"
        )

        # Convert the executed notebook to HTML
        html_exporter = HTMLExporter()
        (body, resources) = html_exporter.from_notebook_node(notebook)

        # Save the HTML output
        with open(OUTPUT_HTML_PATH, "w", encoding="utf-8") as html_file:
            html_file.write(body)
        logger.info(f"Notebook converted to HTML and saved to {OUTPUT_HTML_PATH}")

    except Exception as e:
        logger.error(f"Failed to execute or convert notebook: {e}")


@job
def router_pipeline_job():
    """
    Job to run the Jupyter notebook.
    """
    run_jupyter_notebook()


@schedule(
    cron_schedule="0/10 * * * *",
    job=router_pipeline_job,
)
def router_pipeline_schedule(_context):
    """
    Schedule definition for running the notebook.
    """
    return {}


my_etl_defs = Definitions(
    jobs=[router_pipeline_job], schedules=[router_pipeline_schedule]
)
