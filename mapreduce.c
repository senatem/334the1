#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/wait.h>

int main(int argc, char **argv){

	pid_t pid[atoi(argv[1])], pidr[atoi(argv[1])];		// pid lists for mappers and reducers respectively
	char bf[1023], id[5];								
	int i, j, *child_status, k;
	int mrpipes[atoi(argv[1])][2], rpipes[atoi(argv[1])][2], mpipes[atoi(argv[1])][2];	// pipe lists	

	for(i=0; i<atoi(argv[1]); i++){

		sprintf(id, "%d", i);			// get Mapper/Reducer ID
		if(argc==3){			// if Map model
			pipe(mpipes[i]);	// parent-mapper pipe for Mapper i
			pid[i] = fork();	// fork off mapper i
			
			if(pid[i]){					// if parent					
				close(mpipes[i][0]);	// close read end
			}

			else{										// if mapper
				for(j=0; j<i+1; j++){					// close write ends
					close(mpipes[j][1]);
				}		
				dup2(mpipes[i][0], 0);					// direct read end to stdin
				close(mpipes[i][0]);					// close read end
				execl(argv[2], argv[2], id, NULL);		// execute mapper program
			}
		}
		if(argc==4){			// if MapReduce model
			pipe(mpipes[i]);	// create parent-mapper pipes
			pipe(mrpipes[i]);	// create mapper-reducer pipes
			pipe(rpipes[i]);	// create reducer-reducer pipes

			pid[i] = fork();	// fork off mappers

			if(pid[i]){				// if parent
				pidr[i] = fork();	// fork off reducers
			}

			if(pid[i] && pidr[i]){			// if parent
				close(mpipes[i][0]);		// close read end for parent-mapper pipes
				close(mrpipes[i][0]);		// close mapper-reducer pipes
				close(mrpipes[i][1]);
				close(rpipes[i][1]);		// close write end for reducer-reducer pipes

				if(i){						// if not reducer 0
					close(rpipes[i-1][0]);	// close previous reducer's read end
				}
			}

			else if(pid[i]==0){						// if mapper
				for(j=0; j<i+1; j++){					// close write ends for parent-mapper pipes
					close(mpipes[j][1]);
				}

				close(rpipes[i][0]);				// close reducer-reducer pipes
				close(rpipes[i][1]);
				dup2(mpipes[i][0], 0);				// direct read end of parent-mapper pipe to stdin
				close(mpipes[i][0]);				// close read end for parent-mapper pipe i
				close(mrpipes[i][0]);				// close read end for mapper-reducer pipe i
				dup2(mrpipes[i][1], 1);				// direct write end of mapper-reducer pipe to stdout
				dup2(mrpipes[i][1], 2);				// ...and to stderr
				close(mrpipes[i][1]);				// close write end of mapper-reducer pipe
				
				if(i){								// if not reducer 0
					close(rpipes[i-1][0]);			// close previous reducer's read end
				}				
				
				execl(argv[2], argv[2], id, NULL);	// execute mapper program	
			}

			else{											// if reducer
				if(!i){										// if reducer 0
					dup2(mrpipes[i][0], 2);								// direct stdin to stderr
				}

				else{										// if reducer > 0
					dup2(rpipes[i-1][0], 2);				// direct read end of previous reducer-reducer pipe to stderr
					close(rpipes[i-1][0]);					// close read end of previous reducer-reducer pipe
				}

				if(i!=(atoi(argv[1])-1)){					// if not the last reducer
					dup2(rpipes[i][1], 1);					// direct write end of reducer-reducer pipe to stdout
				}


				for(j=0; j<i+1; j++){						// close write ends of parent-mapper pipes
					close(mpipes[j][1]);
				}

				close(rpipes[i][1]);						// close write end of reducer-reducer pipe
				close(rpipes[i][0]);						// close read end of reducer-reducer pipe
				close(mpipes[i][0]);						// close read end of parent-mapper pipe
				close(mrpipes[i][1]);						// close write end of mapper-reducer pipe
				dup2(mrpipes[i][0], 0);						// direct read end of mapper-reducer pipe to stdin
				close(mrpipes[i][0]);						// close read end of mapper-reducer pipe
				execl(argv[3], argv[3], id, NULL);			// execute reducer program
			}
		}
	}
	
	i = 0;									// initialize mapper counter i = 0
	

	while (fgets(bf,1023,stdin)) {			// get input from stdin into buffer
		k = strlen(bf);
		write(mpipes[i][1], bf, k);		// write buffer content to mapper i
		i++;								// increment i
		if(i==atoi(argv[1])) {i = 0;}		// if out of mappers, reinitialize i
	}

	for(i=0; i<atoi(argv[1]); i++){		// close all remaining pipes
	 	close(mpipes[i][1]);
	}

	for(i=0; i<atoi(argv[1]); i++){		// reap children
	 	wait(child_status);
	}


}
