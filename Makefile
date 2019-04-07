all: mapreduce.o
	gcc -o mapreduce mapreduce.o

mapreduce: mapreduce.c
	gcc -o mapreduce mapreduce.c 
	


clean:
	rm -f *.o
