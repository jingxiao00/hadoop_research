
HADOOP_DEV_HOME = /root/hadoop/hadoop-2.8.0
CC=g++
INC_PATH = -I$(HADOOP_DEV_HOME)/include
LIB_PATH = -L$(HADOOP_DEV_HOME)/lib/native
LIBS = -lhadooppipes -lhadooputils -lpthread
helloworld: helloworld.cpp
	$(CC) $(INC_PATH) $< -lcrypto -lssl -Wall $(LIB_PATH) $(LIBS)  -g -O2 -o $@
clean:
	rm -f *.o helloworld

