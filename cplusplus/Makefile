
HADOOP_DEV_HOME = /root/hadoop/hadoop-2.8.0
CC=g++
CPPFLAGS = -m64
INC_PATH = -I$(HADOOP_DEV_HOME)/include
LIB_PATH = -L$(HADOOP_DEV_HOME)/lib/native
LIBS = -lhadooppipes -lhadooputils -lpthread
max_temperature: max_temperature.cpp
	$(CC) $(CPPFLAGS) $(INC_PATH) $< -lcrypto -lssl -Wall $(LIB_PATH) $(LIBS)  -g -O2 -o $@
clean:
	rm -f *.o max_temperature

