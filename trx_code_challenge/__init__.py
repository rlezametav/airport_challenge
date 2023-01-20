from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    ScheduleDefinition
)
import os
from . import assets

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    schedules=[
        ScheduleDefinition(
            job=define_asset_job(name="daily_refresh", selection="*"),
            cron_schedule="@daily",
        )
    ],
    resources={
        "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
    },
)
