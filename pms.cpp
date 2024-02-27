#include "mpi.h"
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <queue>

#define NUMBERS_FILE_NAME "numbers"
#define DBG 0

#define TAG1 0
#define TAG2 1

#define ASC >
#define DESC <
#define ORDER DESC

void front_pop_dec(std::queue<u_int8_t>* q, uint8_t* n, int* c) {
    *n = q->front();
    q->pop();
    *c = *c-1;
}

int send_numbers() {
    std::ifstream numbers_file(NUMBERS_FILE_NAME, std::ios::binary | std::ios::ate);
    if (!numbers_file) {
        fprintf(stderr, "Cannot open input file.\n");
        MPI_Abort(MPI_COMM_WORLD, -1);
        return -1;
    }

    std::streampos size = numbers_file.tellg();
    numbers_file.seekg(0, std::ios::beg);

    std::vector<uint8_t> arr(size);
    if (!numbers_file.read(reinterpret_cast<char*>(arr.data()), size)) {
        MPI_Abort(MPI_COMM_WORLD, -1);
        return -1;
    }

    for (int i = 0; i < arr.size(); i++) {
        printf("%d ", arr[i]);
    }
    printf("\n");

    int size_int = static_cast<int>(size);
    MPI_Bcast(&size_int, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (size_int > 1) {
        for (int i=0; i < size_int ; i++) {
            MPI_Send(&arr[i], 1, MPI_UINT8_T, 1, i % 2 == 0 ? TAG1 : TAG2, MPI_COMM_WORLD);
            if(DBG) printf("[%d] (%d) Sending %d to %d(Q%d).\n", 0, i, arr[i], 0 + 1, i % 2 == 0 ? TAG1 : TAG2);
        }
    } else {
        if (size_int == 1) printf("%d\n", arr[0]);
        return 1;
    }

    return 0;
}

int main(int argc, char *argv[]) {
    int size, rank;
    u_int8_t n;
    MPI_Init(&argc, &argv);

    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    

    // master process
    if (rank == 0) {
        int ret = send_numbers();
        if (!ret) {
            // wait for finish
            MPI_Recv(&n, 1, MPI_UINT8_T, size - 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        } else {
            if (ret == -1) return -1;
        }
    // last process or middle processes
    } else {
        std::queue<uint8_t> lqs[2];
        
        int numbers_size, pick;
        MPI_Status r;
        MPI_Bcast(&numbers_size, 1, MPI_INT, 0, MPI_COMM_WORLD);
        
        // last process
        if (rank == size - 1 && numbers_size > 1) {
            for (int i=0; i < numbers_size; i++) {
                MPI_Recv(&n, 1, MPI_UINT8_T, rank - 1, MPI_ANY_TAG, MPI_COMM_WORLD, &r);
                lqs[r.MPI_TAG].push(n);
                if (DBG) printf("[%d] Received value %d into Q%d\n", rank + 1, n, r.MPI_TAG);
            }
            
            for (int i=0; i < numbers_size; i++) {
                if (lqs[0].empty()) {
                    pick = 1;
                } else if (lqs[1].empty()) {
                    pick = 0;
                } else {
                    pick = lqs[0].front() ORDER lqs[1].front() ? 0 : 1;
                }
                printf("%d\n", lqs[pick].front());
                lqs[pick].pop();
            }

            MPI_Send(&n, 1, MPI_UINT8_T, 0, 0, MPI_COMM_WORLD);
        }
        
        // middle processes 
        else if (numbers_size > 1) {
            
            bool started = false;
            int count[] = {1 << (rank - 1), 1 << (rank - 1)};
            int sent = 0, received = 0, sent_tag;
            while (sent != numbers_size) {
                if(DBG) printf("[%d] new iteration sent=%d, numbers_size=%d, count0=%d, count1=%d.\n", rank + 1, sent, numbers_size, count[0], count[1]);
                // receive message from the previous process
                if (received != numbers_size) {
                    MPI_Recv(&n, 1, MPI_UINT8_T, rank - 1, MPI_ANY_TAG, MPI_COMM_WORLD, &r);
                    lqs[r.MPI_TAG].push(n);
                    if(DBG) printf("[%d] received new number %d for Q%d (new_size=%lu).\n", rank + 1, n, r.MPI_TAG, lqs[r.MPI_TAG].size());
                    received++;
                }
                
                // ak su fronty zaplnene, nastav priznak zapocatia
                if (!started && lqs[0].size() >= (1 << (rank - 1)) && lqs[1].size() > 0)
                {
                    if(DBG) printf("[%d] Starting sorting.\n", rank + 1);
                    started = true;
                }
                else if (!started) {
                    if(DBG) printf("[%d] Cannot start yet Q0(%lu) Q1(%lu).\n", rank + 1, lqs[0].size(), lqs[1].size());
                }

                if (started) {
                    sent_tag = sent++ % (1 << (rank + 1)) <= ((1 << (rank))-1) ? TAG1 : TAG2;

                    // check if the processor HAS TO pick value from either queues
                    if (!count[0] && !count[1]) {
                        if(DBG) printf("[%d] count1, count2 are zero. Resetting counts.\n", rank + 1);
                        // reset to original values
                        count[0] = 1 << (rank - 1);
                        count[1] = 1 << (rank - 1);
                    }

                    if (!count[0] || !count[1]) {
                        pick = !count[0] ? 1 : 0;
                        front_pop_dec(&lqs[pick], &n, &count[pick]);
                        MPI_Send(&n, 1, MPI_UINT8_T, rank + 1, sent_tag, MPI_COMM_WORLD);
                        
                        if(DBG) printf("[%d] count%d is zero. Sending value %d from Q%d to Q%d.\n", rank + 1, pick ? 0 : 1, n, pick, sent_tag);
                        continue;
                    }
                    
                    pick = (lqs[1].empty() || lqs[0].front() ORDER lqs[1].front()) ? 0 : 1;

                    front_pop_dec(&lqs[pick], &n, &count[pick]);
                    if(DBG) printf("[%d] Sending value %d from Q%d to Q%d\n", rank + 1, n, pick, sent_tag);
                    MPI_Send(&n, 1, MPI_UINT8_T, rank + 1, sent_tag, MPI_COMM_WORLD);
                }
            }
        }
    }
    
    MPI_Finalize();
    return 0;
}