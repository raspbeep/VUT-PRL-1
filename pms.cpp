#include "mpi.h"
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <queue>

#define NUMBERS_FILE_NAME "numbers"
#define DBG 0
char tabs[] = {'\t', '\t', '\t', '\t'};
#define TABS (tabs + _my_rank())

#define TAG1 0
#define TAG2 1

#define ASC >
#define DESC <
#define ORDER DESC

int _my_rank() {
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    return rank + 1;
}

void print_queues(std::queue<u_int8_t> q1, std::queue<u_int8_t> q2) {
    printf("%s [%d] Q0: (", TABS, _my_rank());
    while (!q1.empty()) {
        printf("%d ", q1.front());
        q1.pop();
    }
    printf(") Q1: (");
    while (!q2.empty()) {
        printf("%d ", q2.front());
        q2.pop();
    }
    printf(")\n");
}

void front_pop_dec(std::queue<u_int8_t>* q, uint8_t* n, int* c) {
    *n = q->front();
    q->pop();
    *c = *c-1;
}

int send_numbers() {
    std::ifstream numbers_file(NUMBERS_FILE_NAME, std::ios::binary | std::ios::ate);
    if (!numbers_file) return -1;

    std::streampos size = numbers_file.tellg();
    numbers_file.seekg(0, std::ios::beg);

    std::vector<uint8_t> arr(size);
    if (!numbers_file.read(reinterpret_cast<char*>(arr.data()), size)) {
        return -1;
    }

    for (int i = 0; i < arr.size(); i++) {
        printf("%d ", arr[i]);
    }
    printf("\n");

    int size_int = static_cast<int>(size);
    MPI_Bcast(&size_int, 1, MPI_INT, 0, MPI_COMM_WORLD);

    for (int i=0; i < size_int ; i++) {
        MPI_Send(&arr[i], 1, MPI_UINT8_T, 1, i % 2 == 0 ? TAG1 : TAG2, MPI_COMM_WORLD);
        if(DBG) printf("%s [%d] (%d) Sending %d to %d(Q%d).\n", TABS, _my_rank(), i, arr[i], _my_rank() + 1, i % 2 == 0 ? TAG1 : TAG2);
    }

    return 0;
}

int main(int argc, char *argv[]) {
    int size, rank;
    MPI_Init(&argc, &argv);

    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    u_int8_t n;

    // master process
    if (rank == 0) {
        send_numbers();
        // wait for finish
        MPI_Recv(&n, 1, MPI_UINT8_T, size - 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    } else {
        std::queue<uint8_t> lqs[2];
        
        int numbers_size, pick;
        MPI_Status r;
        MPI_Bcast(&numbers_size, 1, MPI_INT, 0, MPI_COMM_WORLD);
        
        // last process
        if (rank == size - 1) {
            for (int i=0; i < numbers_size; i++) {
                MPI_Recv(&n, 1, MPI_UINT8_T, rank - 1, MPI_ANY_TAG, MPI_COMM_WORLD, &r);
                lqs[r.MPI_TAG].push(n);
                if (DBG) printf("%s [%d] Received value %d into Q%d\n", TABS, rank + 1, n, r.MPI_TAG);
            }

            if (DBG) print_queues(lqs[0], lqs[1]);
            
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
        else {
            
            bool started = false;
            int count[] = {1 << (rank - 1), 1 << (rank - 1)};
            int sent = 0, received = 0, sent_tag;
            while (sent != numbers_size) {
                if(DBG) printf("%s [%d] new iteration sent=%d, numbers_size=%d, count0=%d, count1=%d.\n", TABS, rank + 1, sent, numbers_size, count[0], count[1]);
                // receive message from the previous process
                if (received != numbers_size) {
                    MPI_Recv(&n, 1, MPI_UINT8_T, rank - 1, MPI_ANY_TAG, MPI_COMM_WORLD, &r);
                    lqs[r.MPI_TAG].push(n);
                    if(DBG) printf("%s [%d] received new number %d for Q%d (new_size=%lu).\n", TABS, rank + 1, n, r.MPI_TAG, lqs[r.MPI_TAG].size());
                    received++;
                }
                if (DBG) print_queues(lqs[0], lqs[1]);
                
                // ak su fronty zaplnene, nastav priznak zapocatia
                if (!started && lqs[0].size() >= (1 << (rank - 1)) && lqs[1].size() > 0)
                {
                    if(DBG) printf("%s [%d] Starting sorting.\n", TABS, rank + 1);
                    started = true;
                }
                else if (!started) {
                    if(DBG) printf("%s [%d] Cannot start yet Q0(%lu) Q1(%lu).\n", TABS, rank + 1, lqs[0].size(), lqs[1].size());
                }

                if (started) {
                    sent_tag = sent++ % (1 << (rank + 1)) <= ((1 << (rank))-1) ? TAG1 : TAG2;

                    // check if the processor HAS TO pick value from either queues
                    if (!count[0] && !count[1]) {
                        if(DBG) printf("%s [%d] count1, count2 are zero. Resetting counts.\n", TABS, rank + 1);
                        // reset to original values
                        count[0] = 1 << (rank - 1);
                        count[1] = 1 << (rank - 1);
                    }

                    if (!count[0] || !count[1]) {
                        pick = !count[0] ? 1 : 0;
                        front_pop_dec(&lqs[pick], &n, &count[pick]);
                        MPI_Send(&n, 1, MPI_UINT8_T, rank + 1, sent_tag, MPI_COMM_WORLD);
                        
                        if(DBG) printf("%s [%d] count%d is zero. Sending value %d from Q%d to Q%d.\n", TABS, rank + 1, pick ? 0 : 1, n, pick, sent_tag);
                        continue;
                    }
                    
                    pick = (lqs[1].empty() || lqs[0].front() ORDER lqs[1].front()) ? 0 : 1;

                    front_pop_dec(&lqs[pick], &n, &count[pick]);
                    if(DBG) printf("%s [%d] Sending value %d from Q%d to Q%d\n", TABS, rank + 1, n, pick, sent_tag);
                    MPI_Send(&n, 1, MPI_UINT8_T, rank + 1, sent_tag, MPI_COMM_WORLD);
                }
            }
        }
    }
    
    MPI_Finalize();
    return 0;
}