#include "mpi.h"
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <queue>

#define NUMBERS_FILE_NAME "numbers.bin"
#define ERR -1
#define DBG 1
#define DBG_PRINT(x) if (DBG) {std::cout << x << std::endl;}
#define P_DBG_PRINT(msg) if (DBG) {std::cout << "[" << world_rank << "]" << x << std::endl;}
char tabs[] = {'\t', '\t', '\t', '\t'};
#define TABS (tabs + world_rank)

#define PRINT_Q(q) for (int idx = 0;)

#define TAG1 0
#define TAG2 1
#define world_rank _my_rank()
#define world_size _world_size()

int _my_rank() {
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    return rank + 1;
}

int _world_size() {
    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    return size;
}

void print_queues(std::queue<u_int8_t> q1, std::queue<u_int8_t> q2) {
    printf("%s [%d] Q0: (", TABS, world_rank);
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

int send_numbers() {
    std::ifstream numbers_file;
    numbers_file.open(NUMBERS_FILE_NAME, std::ios::ate | std::ios::binary);
    if (!numbers_file.is_open()) return ERR;

    numbers_file.seekg(0, std::ios::end);
    std::streamsize size = numbers_file.tellg();
    numbers_file.seekg(0, std::ios::beg);

    uint8_t* arr = (uint8_t*)std::malloc(sizeof(uint8_t) * size);

    if (!numbers_file.read(reinterpret_cast<char*>(arr), size)) {
        return ERR;
    }

    int size_int = static_cast<int>(size);
    
    // send the numbers arr size 
    MPI_Bcast(&size_int, 1, MPI_INT, 0, MPI_COMM_WORLD);

    for (int i=0; i < size_int ; i++) {
        MPI_Send(&arr[i], 1, MPI_UINT8_T, 1, i % 2 == 0 ? TAG1 : TAG2, MPI_COMM_WORLD);
        printf("%s [%d] (%d) Sending %d to %d(Q%d).\n", TABS, world_rank, i, arr[i], world_rank + 1, i % 2 == 0 ? TAG1 : TAG2);
    }

    free(arr);
    return 0;
}

int main(int argc, char *argv[]) {

    int size;
    MPI_Init(&argc, &argv);

    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // master process
    if (world_rank == 1) {
        printf("%s [%d] Hello from master process (%d of %d)\n", TABS, world_rank, world_rank, size);
        send_numbers();
        int n;
        // wait for finish
        MPI_Recv(&n, 1, MPI_INT, size - 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Finalize();
    } 

    else if (world_rank == size) {
        printf("%s [%d] Hello from the last process (%d of %d)\n", TABS, world_rank, world_rank, size);
        int numbers_size;
        MPI_Status r;
        MPI_Bcast(&numbers_size, 1, MPI_INT, 0, MPI_COMM_WORLD);
        printf("%s [%d] Last process is waiting for %d numbers.\n", TABS, world_rank, world_size);
        uint8_t n;

        for (int i=0; i < numbers_size; i++) {
            MPI_Recv(&n, 1, MPI_UINT8_T, world_rank - 2, MPI_ANY_TAG, MPI_COMM_WORLD, &r);
            printf("%s [%d] Received value %d into Q%d\n", TABS, world_rank, n, r.MPI_TAG);
        }

        MPI_Send(&n, 1, MPI_UINT8_T, 0, 0, MPI_COMM_WORLD);
        MPI_Finalize();
    }

    else {
        printf("%s[%d] Hello from process (%d of %d)\n", TABS, world_rank, world_rank, size);
        int numbers_size;
        MPI_Status r;
        MPI_Bcast(&numbers_size, 1, MPI_INT, 0, MPI_COMM_WORLD);
        printf("%s [%d] Middle processes are waiting for %d numbers.\n", TABS, world_rank, numbers_size);
        
        std::queue<uint8_t> lqs[2];
        bool started = false;

        u_int8_t n;
        MPI_Status s;
        int count[] = {1 << (world_rank - 2), 1 << (world_rank - 2)};
        int count1 = 1 << (world_rank - 2);
        int count2 = 1 << (world_rank - 2);
        int sent = 0;
        int i = 0;
        int received = 0;
        while (sent != numbers_size) {
            printf("%s [%d] new iteration (%d) sent=%d, numbers_size=%d, count0=%d, count1=%d.\n", TABS, world_rank, i, sent, numbers_size, count[0], count[1]);
            // receive message from the previous process
            if (received != numbers_size) {
                MPI_Recv(&n, 1, MPI_UINT8_T, world_rank - 2, MPI_ANY_TAG, MPI_COMM_WORLD, &s);
                lqs[s.MPI_TAG].push(n);
                printf("%s [%d] received new number %d for Q%d (new_size=%lu).\n", TABS, world_rank, n, s.MPI_TAG, lqs[s.MPI_TAG].size());
                received++;
            }
            print_queues(lqs[0], lqs[1]);
            
            // ak su fronty zaplnene, nastav priznak zapocatia
            if (!started && lqs[0].size() >= (1 << (world_rank - 2)) && lqs[1].size() > 0) {
                printf("%s [%d] Starting sorting.\n", TABS, world_rank);
                started = true;
            } else if (!started) {
                printf("%s [%d] Cannot start yet Q0(%lu) Q1(%lu).\n", TABS, world_rank, lqs[0].size(), lqs[1].size());
            }

            if (started) {
                // check if the processor HAS TO pick value from either queues
                if (!count[0] && !count[1]) {
                    printf("%s [%d] count1, count2 are zero. Resetting counts.\n", TABS, world_rank);
                    // reset to original values
                    count[0] = 1 << (world_rank - 2);
                    count[1] = 1 << (world_rank - 2);
                }
                
                if (!count[0]) {
                    n = lqs[1].front();
                    lqs[1].pop();
                    MPI_Send(&n, 1, MPI_UINT8_T, world_rank, sent % (1 << (world_rank)) <= ((1 << (world_rank-1))-1) ? TAG1 : TAG2, MPI_COMM_WORLD);
                    sent++;
                    count[1]--;
                    printf("%s [%d] count0 is zero. Sending value %d from Q1.\n", TABS, world_rank, n);
                    i++;
                    continue;
                }
                if (!count[1]) {
                    n = lqs[0].front();
                    lqs[0].pop();
                    MPI_Send(&n, 1, MPI_UINT8_T, world_rank, sent % (1 << (world_rank)) <= ((1 << (world_rank-1))-1) ? TAG1 : TAG2, MPI_COMM_WORLD);
                    sent++;
                    count[0]--;
                    printf("%s [%d] count2 is zero. Sending value %d from Q0.\n",TABS,  world_rank, n);
                    i++;
                    continue;
                }
                int pick;
                
                if (lqs[1].empty() || lqs[0].front() > lqs[1].front()) {
                    pick = 0;
                    printf("%s [%d] Sending value %d from Q0\n", TABS, world_rank, n);
                }
                else /*if (lqs[0].empty() || lqs[1].front() > lqs[0].front())*/ {
                    pick = 1;
                    printf("%s [%d] Sending value %d from Q1\n", TABS, world_rank, n);
                }
                n = lqs[pick].front();
                lqs[pick].pop();
                count[pick]--;
                MPI_Send(&n, 1, MPI_UINT8_T, world_rank, sent % (1 << (world_rank)) <= ((1 << (world_rank-1))-1) ? TAG1 : TAG2, MPI_COMM_WORLD);
                sent++;
            }
            i++;
        }
        MPI_Finalize();
    }
    return 0;
}