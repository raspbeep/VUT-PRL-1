/*
 * Pipeline Merge Sort (pms.cpp)
 * Author: Pavel Kratochvil
 * Year: 2024
 * 
 * Program Description:
 * Sorts a list of 8-bit numbers given by the file `numbers` in the same directory.
 * The first output are space delimited unsorted input numbers. The second output
 * are newline delimited sorted numbers from the input. The output order can be
 * reversed by changing the ORDER #define. The program expects to by run with 
 * parameter -np ⌈log2(n)+1⌉, otherwise, the output order of numbers is undefined.
 */

#include "mpi.h"
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <queue>

#define NUMBERS_FILE_NAME "numbers"
#define DBG 0

#define TAG1 0
#define TAG2 1

#define ASC <
#define DESC >
#define ORDER ASC

void front_pop_dec(std::queue<u_int8_t>* q, uint8_t* n, int* c) {
    *n = q->front();
    q->pop();
    *c = *c-1;
}

void first_process() {
    std::ifstream numbers_file(NUMBERS_FILE_NAME, std::ios::binary | std::ios::ate);
    if (!numbers_file) {
        fprintf(stderr, "Cannot open input file.\n");
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    std::streampos size = numbers_file.tellg();
    numbers_file.seekg(0, std::ios::beg);

    std::vector<uint8_t> arr(size);
    if (!numbers_file.read(reinterpret_cast<char*>(arr.data()), size)) {
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    for (int i = 0; i < arr.size(); i++) {
        printf("%d ", arr[i]);
    }
    printf("\n");

    int size_int = static_cast<int>(size);
    MPI_Bcast(&size_int, 1, MPI_INT, 0, MPI_COMM_WORLD);
    // if there are at least two numbers, send to the next processor, otherwise, just print (it's sorted)
    if (size_int > 1) {
        for (int i=0; i < size_int ; i++) {
            MPI_Send(&arr[i], 1, MPI_UINT8_T, 1, i % 2 == 0 ? TAG1 : TAG2, MPI_COMM_WORLD);
            if(DBG) printf("[%d] (%d) Sending %d to %d(Q%d).\n", 0, i, arr[i], 0 + 1, i % 2 == 0 ? TAG1 : TAG2);
        }
    } else {
        if (size_int == 1) printf("%d\n", arr[0]);
    }
}

void middle_processes(int rank, int numbers_size) {
    bool started = false;
    int count[] = {1 << (rank - 1), 1 << (rank - 1)}, sent = 0, received = 0, sent_tag, pick;
    u_int8_t n;
    std::queue<uint8_t> lqs[2];
    MPI_Status r;
    while (sent != numbers_size) {
        if(DBG) printf("[%d] new iteration sent=%d, numbers_size=%d, count0=%d, count1=%d.\n", rank + 1, sent, numbers_size, count[0], count[1]);
        // receive message from the previous process until all numbers are received
        if (received != numbers_size) {
            MPI_Recv(&n, 1, MPI_UINT8_T, rank - 1, MPI_ANY_TAG, MPI_COMM_WORLD, &r);
            lqs[r.MPI_TAG].push(n);
            if(DBG) printf("[%d] received new number %d for Q%d (new_size=%lu).\n", rank + 1, n, r.MPI_TAG, lqs[r.MPI_TAG].size());
            received++;
        }
        
        // when Q0 has at least 2^(rank-1) elements and Q1 has at least 1, the process can start sorting
        if (!started && lqs[0].size() >= (1 << (rank - 1)) && lqs[1].size() > 0) {
            started = true;
            if(DBG) printf("[%d] Starting sorting.\n", rank + 1);
        } else if (!started) {
            if(DBG) printf("[%d] Cannot start yet Q0(%lu) Q1(%lu).\n", rank + 1, lqs[0].size(), lqs[1].size());
        }

        if (started) {
            // tag denoting the next processor queue
            sent_tag = sent++ % (1 << (rank + 1)) <= ((1 << (rank))-1) ? TAG1 : TAG2;

            // both counts are 0, reset them and continue
            if (!count[0] && !count[1]) {
                count[0] = count[1] = 1 << (rank - 1);
                if(DBG) printf("[%d] count1, count2 are zero. Resetting counts.\n", rank + 1);
            }
            
            // check if the processor has to pick value from either queues
            if (!count[0] || !count[1]) {
                pick = !count[0] ? 1 : 0;
                if(DBG) printf("[%d] count%d is zero. Sending value %d from Q%d to Q%d.\n", rank + 1, pick ? 0 : 1, n, pick, sent_tag);
            } else {
                // choose smaller/greater element depending on ORDER
                pick = (lqs[1].empty() || lqs[0].front() ORDER lqs[1].front()) ? 0 : 1;
            }

            front_pop_dec(&lqs[pick], &n, &count[pick]);
            MPI_Send(&n, 1, MPI_UINT8_T, rank + 1, sent_tag, MPI_COMM_WORLD);
            if(DBG) printf("[%d] Sending value %d from Q%d to Q%d\n", rank + 1, n, pick, sent_tag);
        }
    }
}

void last_process(int rank, int numbers_size) {
    u_int8_t n;
    int pick;
    std::queue<uint8_t> lqs[2];
    MPI_Status r;

    // receive all numbers from the last but one process and save them
    for (int i=0; i < numbers_size; i++) {
        MPI_Recv(&n, 1, MPI_UINT8_T, rank - 1, MPI_ANY_TAG, MPI_COMM_WORLD, &r);
        lqs[r.MPI_TAG].push(n);
        if (DBG) printf("[%d] Received value %d into Q%d\n", rank + 1, n, r.MPI_TAG);
    }
    
    // print sorted list of numbers
    for (int i=0; i < numbers_size; i++) {
        // if either queue is empty, pick from the other one
        if (lqs[0].empty()) {
            pick = 1;
        } else if (lqs[1].empty()) {
            pick = 0;
        } else {
            // pick smaller/greater element depending on ORDER
            pick = lqs[0].front() ORDER lqs[1].front() ? 0 : 1;
        }
        printf("%d\n", lqs[pick].front());
        lqs[pick].pop();
    }
}

int main(int argc, char *argv[]) {
    int size, rank;    
    MPI_Init(&argc, &argv);

    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // master process
    if (rank == 0) {
        u_int8_t n;
        first_process();
    } else {
        int numbers_size;
        MPI_Bcast(&numbers_size, 1, MPI_INT, 0, MPI_COMM_WORLD);
        if (numbers_size > 1) {
            // last process
            if (rank == size - 1) {
                last_process(rank, numbers_size);
            }
            // middle processes 
            else {
                middle_processes(rank, numbers_size);
            }
        }
    }
    
    MPI_Finalize();
    return 0;
}