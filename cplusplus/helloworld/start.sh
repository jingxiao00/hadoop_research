
mapred pipes -D mapreduce.pipes.isjavarecordreader=true -D mapreduce.pipes.isjavarecordwriter=true -input /helloworld/tmp.txt -output output -program /helloworld/helloworld
