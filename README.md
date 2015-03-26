# Ignition-Template

This provides a template to work with [Apache Spark](http://spark.apache.org/) and with [Ignition-Core](https://github.com/chaordic/ignition-core/).

## Notable features

* This template will point to a stable Spark and Hadoop-client version, potentially working-around some known bugs
* `src/main/scala/ignition/jobs/Runner.scala` is a example of multi-setup project that can run using `core/tools/cluster.py jobs run` command
* See `scripts/job_runner.py` for a full-featured, production-grade script-template that launchs and supervises clusters and jobs using AWS spot instances with automatic failure recovery

In general, check the source files for more information.

## Getting started
See http://monkeys.chaordic.com.br/start-using-spark-with-ignition/ for quick-start tutorial

## TODO

* An example using [Scalaz](https://github.com/scalaz/scalaz) Validation
* Other examples, more documentation
