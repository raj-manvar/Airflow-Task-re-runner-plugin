import os
import re
import subprocess

import airflow
import pendulum
from airflow.models.taskfail import TaskFail
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.db import provide_session
from airflow.www import utils as wwwutils
from flask import Blueprint, redirect, flash
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from flask_wtf import FlaskForm
from sqlalchemy import and_
from wtforms import (
    StringField,
    IntegerField,
    SubmitField,
    TextAreaField,
    RadioField,
)
from wtforms.validators import DataRequired
from plugins.PluginUtils import PluginUtils

airflow.load_login()
login_required = airflow.login.login_required


class TaskRerunForm(FlaskForm):
    logs_regex = StringField("Logs regex", validators=[DataRequired()])
    dag_id_regex = StringField(
        "Dag ID regex", validators=[DataRequired()], default=".*"
    )
    task_id_regex = StringField(
        "Task ID regex", validators=[DataRequired()], default=".*"
    )
    duration = IntegerField("Age in hours", validators=[DataRequired()])
    submit = SubmitField("Submit")


class ConfirmationForm(FlaskForm):
    tasks_list = TextAreaField(render_kw={"cols": "100%"})
    clear_downstream = RadioField(
        label="Clear downstream tasks",
        choices=["Yes", "No"],
        validators=[DataRequired()],
    )
    submit_button = SubmitField("Submit")


def get_last_try_task_logs(failed_task, airflow_db_session):
    return PluginUtils.get_task_logs(
        failed_task.dag_id,
        failed_task.task_id,
        pendulum.instance(failed_task.execution_date),
        airflow_db_session,
    )[0][-1]


def get_failed_tasks_in(
    airflow_db_session, hours, logs_regex, dag_id_regex, task_id_regex
):
    all_failed_tasks = airflow_db_session.query(TaskFail).filter(
        and_(
            TaskFail.start_date >= pendulum.now().subtract(hours=hours),
            TaskFail.dag_id.op("REGEXP")(dag_id_regex),
            TaskFail.task_id.op("REGEXP")(task_id_regex),
        )
    )
    return [
        failed_task
        for failed_task in all_failed_tasks
        if re.findall(
            logs_regex, get_last_try_task_logs(failed_task, airflow_db_session)
        )
    ]


def get_string_from_task(task):
    return "      ".join(
        [task.dag_id, task.task_id, str(pendulum.instance(task.execution_date))]
    )


def get_airflow_clear_command(task_str, clear_downstream):
    dag_id, task_id, execution_date = task_str.split()
    command = f"airflow clear {dag_id} --task_regex {task_id} --start_date {execution_date} --end_date {execution_date} --no_confirm"
    if clear_downstream:
        command += " --downstream "
    return command


class TaskRerunner(AppBuilderBaseView):
    default_view = "render"

    @expose("/", methods=["GET", "POST"])
    @login_required
    @wwwutils.action_logging
    @provide_session
    def render(self, session=None):
        task_rerun_form = TaskRerunForm()
        confirmation_form = ConfirmationForm()

        if task_rerun_form.validate_on_submit():
            failed_tasks = get_failed_tasks_in(
                session,
                task_rerun_form.duration.data,
                task_rerun_form.logs_regex.data,
                task_rerun_form.dag_id_regex.data,
                task_rerun_form.task_id_regex.data,
            )

            failed_tasks_str = "\n".join(
                set([get_string_from_task(failed_task) for failed_task in failed_tasks])
            )
            confirmation_form = ConfirmationForm(data={"tasks_list": failed_tasks_str})
            confirmation_form.tasks_list.render_kw.update(
                {"rows": failed_tasks_str.count("\n") + 5}
            )
            return self.render_template(
                "task_rerunner_plugin/confirmation_form.html", form=confirmation_form
            )

        if confirmation_form.validate_on_submit():
            failed_tasks_str = confirmation_form.tasks_list.data
            clear_downstream = confirmation_form.clear_downstream.data == "Yes"
            clear_command = "\n".join(
                get_airflow_clear_command(task_str, clear_downstream)
                for task_str in failed_tasks_str.split("\n")
            )
            print(f"Rerunner tool running clear command: {clear_command}")
            subprocess.Popen(clear_command, shell=True)
            flash("Request sent. Monitor tasks and webserver logs to see status.")
            return redirect(os.getenv("AIRFLOW__WEBSERVER__BASE_URL"))

        return self.render_template(
            "task_rerunner_plugin/rerunner_form.html",
            task_rerun_form=TaskRerunForm(),
            title="Clears tasks based on regex in logs and duration of logs.",
        )


v_appbuilder_package = {
    "name": "Worker Logs",
    "category": "Admin",
    "view": TaskRerunner(),
}

bp = Blueprint(
    "task_rerunner_plugin",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/template",
)


class AirflowWorkerLogsPlugin(AirflowPlugin):
    name = "task_rerunner_plugin"
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]
