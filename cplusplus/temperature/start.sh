
hadoop pipes -Dhadoop.pipes.java.recordreader=true -D hadoop.pipes.java.recordwriter=true -input /max_temperature/sample.txt -output output -program /max_temperature/max_temperature

