from dagster import AssetSelection, RunRequest, ScheduleEvaluationContext, asset, define_asset_job, op, job, ScheduleDefinition, schedule, DefaultScheduleStatus


# jobをschedulingする方法はいくつかある
# AssetJobについて説明する(opに関してもjobの定義の仕方が変わるだけなので割愛する)

@asset(
    group_name="schedule_job"
)
def schedule_asset():
    return 1

schedule_asset_job = define_asset_job(
    "schedule_asset_job",
    selection=AssetSelection.groups("schedule_job"),
)

# ScheduleDefinitionを定義することでジョブのスケジュールを定義することができる
# スケジュールはcron式である。
# ちなみにこの結果をdefinitionに渡しただけでは有効にならないことに注意
scheduled_asset_job = ScheduleDefinition(
    job=schedule_asset_job,
    cron_schedule="0 0 * * *",
    # この価をRUNNINGにすると、デフォルトでスケジュールが有効になる
    default_status=DefaultScheduleStatus.STOPPED
)

# また、scheduleデコレータでも定義することができる
# この場合は、スケジュール実行特有の処理を行うことができる
# 例えば、実行タイミングに依存する変数を設定するなど
@schedule(
    job=schedule_asset_job,
    cron_schedule="0 0 * * *"    
)
def configured_scheduled_asset_job(context: ScheduleEvaluationContext):
    execute_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    context.log.info(f"実行日: {execute_date}")
    
    return RunRequest(
        run_key=None,
        tags={"date": execute_date}
    )

