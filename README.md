# Ignition-Template

This provides a template to work with Spark and with Ignition-Core.

## Notable features

* This template will point to a stable Spark and Hadoop-client version, potentially working-around some known bugs
* `src/main/scala/ignition/jobs/Runner.scala` is a example of multi-setup project that can run using `core/tools/cluster.py jobs run` command
* See scripts/job_runner.py for a full-featured script-template that launchs and supervises cluster and jobs using AWS spot instaces with automatic recovery

In general, check the source files for more information
