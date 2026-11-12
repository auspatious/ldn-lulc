import typer

from ldn.lulc import run_classify_task

classify_app = typer.Typer()

classify_app.command("run")(run_classify_task)
