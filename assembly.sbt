import AssemblyKeys._ // put this at the top of the file

assemblySettings

assemblyOption in assembly ~= { _.copy(includeScala = false) }

mainClass in assembly := Some("ignition.jobs.Runner")

test in assembly := {}

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
runMain in Compile <<= Defaults.runMainTask(fullClasspath in Compile, runner in (Compile, run))
