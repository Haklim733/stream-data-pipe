<!--
FROM apache flink github
NOT READY FOR USE
-->

run `./jars.sh` to download all the necessary jars to the jars folder which will be mounted to the flink cluster

Additional optional Flink arguments:

- `--checkpoint` - Set a checkpoint interval in milliseconds (default: 10000)
- `--event_interval` - Set a time in milliseconds to sleep between each randomly generated record (default: 5000)
