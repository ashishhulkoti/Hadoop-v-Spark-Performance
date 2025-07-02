#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <stdint.h>
#include "blake3.h"
#include <stdio.h>
#include <unistd.h>
#include <openssl/rand.h>
#include <inttypes.h>
#include <stdbool.h>
#define MAX_THREAD  16
#define NONCE_SIZE 6
#define HASH_SIZE 10

unsigned long int FILE_BUFFER_SIZE;

typedef struct Record
{
    uint8_t hash[HASH_SIZE];   // hash value as byte array
    uint8_t nonce[NONCE_SIZE]; // Nonce value as byte array
} Record;

Record *records;

typedef struct {
    const char **input_filenames;
    int num_files;
    const char *output_filename;
    size_t memory_limit;
} ThreadArgs;

typedef struct {
    Record record;
    int file_index;
} HeapNode;

typedef struct {
    uint64_t s[2];
} xorshift128plus_state;



void *threadFunction();
void swap(Record *a, Record *b);
void quicksort(Record *array, int start, int end, int id);

typedef struct t_object Object;
struct t_object {
	int right, left;
	Record *array;
	Object *next;
};
int nbThread = 0, counter = 1;
pthread_t threads[MAX_THREAD];
pthread_spinlock_t lock;

struct timeval start, end;
unsigned long elapsed_milliseconds=0;
char *final_op;
unsigned long int num_recs_g;
int part = 0;
int FILE_SIZE; // Size of the file to be generated in GB
int NUM_THREADS_g;
int NUM_THREADS_s;
int NUM_THREADS_w;
unsigned long int MEM_SIZE;           // Memeory size available in MB
unsigned long int MEM_SIZE_B; // RAM size in Bytes
                              // MEM_SIZE_B=512;
unsigned long int TOTAL_RECORDS;
unsigned long int RECORDS_CHUNK_SIZE; // Number of records that will fit in the memory
int NUM_FILES;                        // Number of times limited Memory will be reused
bool debug=false;

//Function to compare Hash
int compareHash(const uint8_t *a, const uint8_t *b, size_t size) {
    for (size_t i = 0; i < size; i++) {
        if (a[i] < b[i]) return -1;
        if (a[i] > b[i]) return 1;
    }
    return 0;
}

//Funtion to compare Records
int compare_records(const void *a, const void *b) {
    return memcmp(a, b, sizeof(Record));
}

void merge(int low, int mid, int high)
{
    int n1 = mid - low + 1;
    int n2 = high - mid;
    Record *left = (Record *)malloc(n1 * sizeof(Record));
    Record *right = (Record *)malloc(n2 * sizeof(Record));
    int i, j, k;
    for (i = 0; i < n1; i++)
        left[i] = records[i + low];
    for (j = 0; j < n2; j++)
        right[j] = records[j + mid + 1];
    i = j = 0;
    k = low;
    while (i < n1 && j < n2)
    {
        if (memcmp(left[i].hash, right[j].hash, HASH_SIZE) <= 0)
        {
            records[k++] = left[i++];
        }
        else
        {
            records[k++] = right[j++];
        }
    }

    while (i < n1)
    {
        records[k++] = left[i++];
    }
    while (j < n2)
    {
        records[k++] = right[j++];
    }
    free(left);
    free(right);
}

void merge_sort(int low, int high)
{
    int mid = low + (high - low) / 2;
    if (low < high)
    {
        merge_sort(low, mid);
        merge_sort(mid + 1, high);
        merge(low, mid, high);
    }
}

void *merge_sort_thread(void *arg)
{
    int thread_part = part++;
    int low = thread_part * (RECORDS_CHUNK_SIZE / NUM_THREADS_s);
    int high = (thread_part + 1) * (RECORDS_CHUNK_SIZE / NUM_THREADS_s) - 1;
    int mid = low + (high - low) / 2;

    if (low < high)
    {
        merge_sort(low, mid);
        merge_sort(mid + 1, high);
        merge(low, mid, high);
    }

    return NULL;
}

uint64_t xorshift128plus(xorshift128plus_state *state) {
    uint64_t s1 = state->s[0];
    const uint64_t s0 = state->s[1];
    state->s[0] = s0;
    s1 ^= s1 << 23;
    state->s[1] = s1 ^ s0 ^ (s1 >> 18) ^ (s0 >> 5);
    return state->s[1] + s0;
}

void *generate_blake3(void *arg){

	int i = * (int *) arg;
	uint64_t x;
	unsigned long int limit = (i + 1) * num_recs_g;
	unsigned long int j=i*num_recs_g;
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
	srand(time(NULL));
	for (; j < limit; j++)
    {
		RAND_bytes(records[j].nonce, NONCE_SIZE);
		
		blake3_hasher_update(&hasher, records[j].nonce, NONCE_SIZE);
        blake3_hasher_finalize(&hasher, records[j].hash, HASH_SIZE);
    }
	return NULL;
}
//Function to generate the hashes
void generate_hashes()
{
	int id[NUM_THREADS_g];
	num_recs_g = RECORDS_CHUNK_SIZE/NUM_THREADS_g;
	pthread_t threads[NUM_THREADS_g];
    	for (int i = 0; i < NUM_THREADS_g; i++) {
		id[i]=i;
        	pthread_create(&threads[i], NULL, generate_blake3, (void *) &id[i]);
    	}
    	for (int i = 0; i < NUM_THREADS_g; i++) {
        	pthread_join(threads[i], NULL);
    	}
}

//Function to sort the data
void sort_subfile_hashes(){
    pthread_t threads[NUM_THREADS_s];
        for (int i = 0; i < NUM_THREADS_s; i++)
        {
            pthread_create(&threads[i], NULL, merge_sort_thread, (void *)NULL);
        }
        for (int i = 0; i < NUM_THREADS_s; i++)
        {
            pthread_join(threads[i], NULL);
        }
        int n = RECORDS_CHUNK_SIZE / NUM_THREADS_s;
        for (int i = 1; i < NUM_THREADS_s; i *= 2)
        {
            for (int j = 0; j < NUM_THREADS_s; j += 2 * i)
            {
                int low = j * n;
                int mid = (j + i) * n - 1;
                int high = (j + 2 * i) * n - 1;
                if (high >= RECORDS_CHUNK_SIZE)
                    high = RECORDS_CHUNK_SIZE - 1;
                if (mid < high)
                    merge(low, mid, high);
            }
        }

}


void quicksort(Record *array, int start, int end, int id) {
    if (start >= end) return;
    int left = start, right = end + 1;
    const Record pivot = array[start];

    while (1) {
        do {
            right--;
        } while (right >= 0 && compareHash(array[right].hash, pivot.hash, HASH_SIZE) > 0);
        do {
            left++;
        } while (left < RECORDS_CHUNK_SIZE && compareHash(array[left].hash, pivot.hash, HASH_SIZE) < 0);
        if (left < right) {
            swap(&array[left], &array[right]);
        } else {
            break;
        }
    }
    swap(&array[start], &array[right]); // swap the pivot

    if (nbThread < MAX_THREAD && ((end - right) * 100 / RECORDS_CHUNK_SIZE) >= 100 / MAX_THREAD) {
        Object *doubleganger = malloc(sizeof(Object));
        doubleganger->left = right + 1;
        doubleganger->right = end;
        doubleganger->array = array;
        if (pthread_create(&threads[nbThread++], NULL, &threadFunction, (void *)doubleganger) == -1) {
            exit(EXIT_FAILURE);
        }
        quicksort(array, start, right - 1, id);
    } else {
        quicksort(array, start, right - 1, id);
        quicksort(array, right + 1, end, id);
    }
}


void *threadFunction(void *arg) {
	int id, i;
	for(i = 0; i < NUM_THREADS_s; i++) {
		if(pthread_equal(threads[i], pthread_self()) != 0) id = i;
	}
	Object *actuel = (Object*)arg;
	quicksort(actuel->array, actuel->left, actuel->right, id);
	pthread_exit(NULL);
}


void swap(Record *a, Record *b) {
	Record t = *a;
	*a = *b;
	*b = t;
}


void qsort_subfile_hashes(){

    srand(time(NULL));
    pthread_spin_init(&lock, PTHREAD_PROCESS_PRIVATE);
    int mean = 0;
    struct timeval start, end;

    nbThread = 1;

    Object *doubleganger = malloc(sizeof(Object));
    doubleganger->left = 0, doubleganger->right = RECORDS_CHUNK_SIZE - 1;
    doubleganger->array = records, doubleganger->next = NULL;


    if (pthread_create(&threads[0], NULL, &threadFunction, (void *)doubleganger) == -1) exit(EXIT_FAILURE);
    for (int i = 0; i < MAX_THREAD; i++) pthread_join(threads[i], NULL);
    
    // Verification can be added here
    free(doubleganger);

}


void swap_heap_nodes(HeapNode *a, HeapNode *b) {
    HeapNode temp = *a;
    *a = *b;
    *b = temp;
}

void heapify(HeapNode *heap, int n, int i) {
    int smallest = i;
    int left = 2 * i + 1;
    int right = 2 * i + 2;

    if (left < n && compare_records(&heap[left].record, &heap[smallest].record) < 0) {
        smallest = left;
    }

    if (right < n && compare_records(&heap[right].record, &heap[smallest].record) < 0) {
        smallest = right;
    }

    if (smallest != i) {
        swap_heap_nodes(&heap[i], &heap[smallest]);
        heapify(heap, n, smallest);
    }
}

void build_heap(HeapNode *heap, int n) {
    for (int i = n / 2 - 1; i >= 0; i--) {
        heapify(heap, n, i);
    }
}

//Function to merge files assigned to thread
void merge_files(const char *output_filename, const char *input_filenames[], int num_files, size_t memory_limit) { 
    FILE *output_file = fopen(output_filename, "wb");
    if (!output_file) {
        perror("Failed to open output file");
        exit(EXIT_FAILURE);
    }
    // Set a larger buffer for the file stream
    Record *buff = (Record *)malloc(memory_limit);
    unsigned int rec_limit=memory_limit/16;
    unsigned int recs_curr=0;
    FILE **input_files = malloc(num_files * sizeof(FILE *));
    HeapNode *heap = malloc(num_files * sizeof(HeapNode));

    // Open all input files and read the first record from each
    for (int i = 0; i < num_files; ++i) {
        input_files[i] = fopen(input_filenames[i], "rb");
        if (!input_files[i]) {
            perror("Failed to open input file");
            exit(EXIT_FAILURE);
        }
        fread(&heap[i].record, sizeof(Record), 1, input_files[i]);
        heap[i].file_index = i;
    }

    build_heap(heap, num_files);

    // Merge records from input files
    while (num_files > 0) {
        // Write the smallest record to the output file
    
        buff[recs_curr]=heap[0].record;
        if(++recs_curr == rec_limit)
        {
            fwrite(buff,sizeof(Record),rec_limit,output_file);   
            recs_curr=0;
        }
        // Read the next record from the file that had the smallest record
        if (fread(&heap[0].record, sizeof(Record), 1, input_files[heap[0].file_index]) != 1) {
            fclose(input_files[heap[0].file_index]);
            input_files[heap[0].file_index] = NULL;
            heap[0] = heap[num_files - 1];
            num_files--;
        }

        heapify(heap, num_files, 0);
    }

    // Clean up
    free(heap);
    free(input_files);
    free(buff);
    fclose(output_file);
}

void *thread_merge_files(void *arg) {
    ThreadArgs *args = (ThreadArgs *)arg;
    merge_files(args->output_filename, args->input_filenames, args->num_files, args->memory_limit);
    return NULL;
}

//Function to sort and merge multiple sorted subfiles
void write_final_file(){
    
    pthread_t threads[NUM_THREADS_w];
    ThreadArgs thread_args[NUM_THREADS_w];
    size_t memory_limit_per_thread = MEM_SIZE_B / NUM_THREADS_w;
    int actual_NUM_THREADS_w = (NUM_FILES < NUM_THREADS_w) ? NUM_FILES : NUM_THREADS_w; // Use the smaller of NUM_FILES or NUM_THREADS_w
    int files_per_thread = NUM_FILES / actual_NUM_THREADS_w;

    // Prepare input file names and create threads
    for (int i = 0; i < actual_NUM_THREADS_w; ++i) {
        thread_args[i].input_filenames = malloc(files_per_thread * sizeof(char *));
        for (int j = 0; j < files_per_thread; ++j) {
            int file_index = i * files_per_thread + j;
            thread_args[i].input_filenames[j] = malloc(64 * sizeof(char));
            sprintf((char *)thread_args[i].input_filenames[j], "./tmp/temp_data_%d.bin", file_index);
        }
        thread_args[i].num_files = files_per_thread;
        thread_args[i].output_filename = malloc(64 * sizeof(char));
        sprintf((char *)thread_args[i].output_filename, "./tmp/partial_sorted%d.bin", i);
        thread_args[i].memory_limit = memory_limit_per_thread;

        pthread_create(&threads[i], NULL, thread_merge_files, &thread_args[i]);
    }

    // Wait for threads to finish
    for (int i = 0; i < actual_NUM_THREADS_w; ++i) {
        pthread_join(threads[i], NULL);
    }

    // Merge partial sorted files
    const char *partial_sorted_filenames[actual_NUM_THREADS_w];
    for (int i = 0; i < actual_NUM_THREADS_w; ++i) {
        partial_sorted_filenames[i] = thread_args[i].output_filename;
    }
    merge_files(final_op, partial_sorted_filenames, actual_NUM_THREADS_w, MEM_SIZE_B);

    // Free allocated memory
    for (int i = 0; i < actual_NUM_THREADS_w; ++i) {
        for (int j = 0; j < files_per_thread; ++j) {
            free((void *)thread_args[i].input_filenames[j]);
        }
        free((void *)thread_args[i].input_filenames);
        free((void *)thread_args[i].output_filename);
    }

}


int main(int argc, char* argv[])
{
    int opt;
   // Parse command line arguments
    while ((opt = getopt(argc, argv, "t:o:i:f:m:s:d:")) != -1) {
        switch (opt) {
            case 't':
                NUM_THREADS_g = atoi(optarg);
                break;
            case 'o':
                NUM_THREADS_s = atoi(optarg);
                break;
            case 'i':
                NUM_THREADS_w = atoi(optarg);
                break;
            case 'f':
                final_op = optarg;
                break;
            case 'm':
                MEM_SIZE = atoi(optarg);
                break;
            case 's':
                FILE_SIZE = atoi(optarg);
                break;
	    case 'd':
		if(strcmp(optarg,"true")==0) debug=true;
		else debug=false;
		break;
            case '?':
                printf("\nInvalid arguments\n");
		exit(EXIT_FAILURE);
        }
    }	
    
    printf("NUM_THREADS_HASH=%d\nNUM_THREADS_SORT=%d\nNUM_THREADS_WRITE=%d\nFILENAME=%s\nMEMORY_SIZE=%ldMB\nFILESIZE=%dMB\nRECORD_SIZE=16B\nHASH_SIZE=10B\nNONCE_SIZE=6B\n",NUM_THREADS_g,NUM_THREADS_s,NUM_THREADS_w,final_op,MEM_SIZE,FILE_SIZE);

    MEM_SIZE_B = MEM_SIZE * 1024 * 1024;
    TOTAL_RECORDS = ((FILE_SIZE * 1024 * 1024) / 16);
    RECORDS_CHUNK_SIZE = MEM_SIZE_B / 16;      // Number of records that will fit in the memory
    NUM_FILES = (FILE_SIZE) / MEM_SIZE; // Number of times limited Memory will be reuse
    int i;
    time_t gen_begin, gen_end, sort_begin, sort_end, write_begin, write_end;
    double gen_time_spent = 0, sort_time_spent = 0, write_time_spent = 0;
    time_t total_begin, total_end;
    double total_time_spent = 0;

    time(&total_begin);
    records = (Record *)malloc(RECORDS_CHUNK_SIZE * sizeof(Record));
    srand(time(NULL));

    //Loop to generate sorted subfiles
    for (int c = 0; c < NUM_FILES; c++)
    {
        part = 0;
        if(debug) printf("\nGenerating %d hash set\n", c);
        time(&gen_begin);
        gettimeofday(&start, NULL);
	    generate_hashes();
        time(&gen_end);
        gen_time_spent = gen_time_spent + (double)(gen_end - gen_begin);
        if(debug) printf("\nCompleted %d hash set generation\n", c);

        // Sorting and writing to sub file
        time(&sort_begin);
	    if(debug) printf("\nSorting %d hash set\n", c);

        sort_subfile_hashes();
        
        char filename[50];
        sprintf(filename, "./tmp/temp_data_%d.bin", c);
        FILE *file = fopen(filename, "wb");
        if (file == NULL)
        {
            perror("Error creating file");
            return 1;
        }
		
	    // Set a larger buffer for the file stream
        fwrite(records, sizeof(Record), RECORDS_CHUNK_SIZE, file);
        gettimeofday(&end, NULL);
	    elapsed_milliseconds += (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
	    time(&sort_end);
        sort_time_spent = sort_time_spent + (double)(sort_end - sort_begin);
        fclose(file);
	    if(debug) printf("\nSorted %d hash set\n", c);
    }
    free(records);

    // Full sort and merge the files into one file
 
    if(debug) printf("\nStarting file writing\n");
    
    time(&write_begin);
    
    write_final_file();  
    
    if(debug) printf("\nFinished File writing\n");
    time(&write_end);
    gettimeofday(&start, NULL);
    write_time_spent = write_time_spent + (double)(write_end - write_begin);
    gettimeofday(&end, NULL);
    elapsed_milliseconds += (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
    time(&total_end);
    total_time_spent = (double)(total_end - total_begin);
    FILE *tfile = fopen("time.txt", "a");
    fprintf(tfile, "%d,%d,%d,%.3f\n", NUM_THREADS_g, NUM_THREADS_s, NUM_THREADS_w,elapsed_milliseconds/(float)1000);
    fclose(tfile);
    printf("\nFile generated successfully in %fs!\n",elapsed_milliseconds/(float)1000);
    return 0;
}



