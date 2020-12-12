#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#include <string.h>

#include <sys/prctl.h>
#include <pthread.h>
#include <sys/stat.h>

#define MAX_WORDS           1024
#define MAX_CHAR_PER_WORD   100

typedef struct details {
    unsigned long       hash_value[MAX_WORDS];
    uint32_t            wcount[MAX_WORDS];
    uint32_t            total_wcount;
    uint32_t            seq;
    FILE                *fmain;
    FILE                *fsub;
    char                words[MAX_WORDS][MAX_CHAR_PER_WORD];
    char                *fname_main;
    char                *fname_sub;
    pthread_mutex_t     rw_lock;
    pthread_t           s_tid;
}details;

char *fname;

details g_details = {.total_wcount = 0};

void populate_the_words(char *fname);
void *read_search_and_count();
void display_stats();
void reset_counter();
unsigned long get_hash_value(unsigned char *str);
unsigned long is_file_modified(uint8_t is_main_file, unsigned long lmtime);
int hash_binary_search (unsigned long hash_eq, int low, int high);
void rearrange_string_hash_elemets ();
uint8_t is_duplicate(char *word);

#define print_the_words(a)   \
    for (i = 0; i < a->total_wcount; i++) { \
        printf("Words %s counts %d\n", a->words[i], a->wcount[i]); \
    } \
        
void main(int argc, char *argv[])
{
    details *det        = &g_details;
    int     i;
    char    ch;
    char    fname[100];
    uint8_t disp        = 1;

    if (argc < 3) {
        printf("Please enter the the file names\n");
        printf("order\n 1. main file 2. sub-file\n");
        exit(0);
    }
    det->fname_main = argv[1];
    det->fname_sub  = argv[2];
    det->fmain      = NULL;
    det->fsub       = NULL;

#ifdef DEBUG
    print_the_words(det)
#endif /*UT_DEBUG*/

    if (0 != pthread_mutex_init(&det->rw_lock, NULL)) {
        printf("Error, in infra init\n");
    }

    /* populate words from sub file into a
     * global variable */
    populate_the_words(det->fname_sub);

    printf("Thread no created\n");
    if (0 != pthread_create(&det->s_tid, NULL,
                        read_search_and_count, NULL)) {
        printf("Error, pthread create failed Exiting\n");
        exit(0);
    }
    printf("Thread created\n");
    while (1) {
        if (disp) {
            disp = 0;
            printf("Enter 1 to change the sub file\n");
            printf("Enter 2 to reset the counter\n");
            printf("Enter 3 to get the stats\n");
            printf("Enter 4 to exit\n");
            fflush (stdout);
        }
        ch = getchar();
        switch(ch) {
            case '1':
                scanf("%s",fname);
                populate_the_words(fname);
                disp = 1;
                break;
            case '2':
                reset_counter();
                disp = 1;
                break;
            case '3':
                display_stats();
                disp = 1;
                break;
            case '4':
                /*intiname the main thread*/
                display_stats();
                exit(0);
            default:
                break;
        }
        usleep(100000);
    }
    exit(0);
}

void
display_stats()
{
    int i = 0;
    details *det = &g_details;
 
    printf("The count of matches of each word are: \n");
    printf("************START*************** \n");
    pthread_mutex_lock(&det->rw_lock);
    while(i < det->total_wcount)  {
        printf("%-15s      : %d Times\n", det->words[i], det->wcount[i]);
        i++;
    }
    pthread_mutex_unlock(&det->rw_lock);
    printf("************END*************** \n");
    return;
}

#if 0
void user_interface ()
{
    while(1) {
        printf("Enter 1 to change the sub file\n");
        printf("Enter 2 to reset the counter\n");
        printf("Enter 3 to get the stats\n");
        printf("Enter 4 to exit\n");
        fflush ( stdout );
        ch = getchar();
        switch('ch') {
            case '1':
                char *name;
                scanf("%s", name);
                populate_the_words(name);
                break;
            case '2':
                reset_counter();
            case '3':
                display_stats();
            case '4':
                /*intiname the main thread*/
                exit(0);
            default:
                break;
        }
    } 
    return;
}
#endif /*if 0*/

void populate_the_words(char *fname)
{
    details     *det = &g_details;
    FILE        *f2;
    int         i = 0, j = 0;
    struct stat attr;

    /*take the lock*/
    pthread_mutex_lock(&det->rw_lock);
    det->fname_sub = fname;
    /* if the file is opened already close it
     * and open the fname passed */
    if (det->fsub != NULL) {
        fclose(det->fsub);
        det->fsub = NULL;
    }
    det->total_wcount = 0;
    det->fname_sub = fname;

    /*check if the file is empty*/
    stat(det->fname_sub, &attr);
    if( attr.st_size ==0 ){
        printf("Error, Sub file is empty\n");
        pthread_mutex_unlock(&det->rw_lock);
        return;
    }
    det->fsub = fopen(det->fname_sub, "r");
    if (det->fsub == NULL) {
        printf("Error, file open failed\n");
        exit(1);
    }
    while (1) {
        char ch = fgetc(det->fsub);
        if (ch == EOF) {
            fclose(det->fsub);
            det->fsub = NULL;
            break;
        }
        if (ch != ' ' && ch != '\n') {
            det->words[i][j++] = ch; 
            continue;
        } else {
            det->words[i][j] = '\0';
            /*ignore the duplicates*/
            if (is_duplicate(det->words[i]) == 1) {
                j = 0;
                continue;
            }
            det->hash_value[i] = get_hash_value(det->words[i]);
            det->total_wcount++;
            i++;
            j = 0;
            if (i > MAX_WORDS) {
                printf("Error, file having no. of words %d:\n", i);
                printf("Error, words read: %d\n", MAX_WORDS);
                break;
            }
        }
    }
    rearrange_string_hash_elemets();
    /*release_lock*/
    pthread_mutex_unlock(&det->rw_lock);
    return;
}

unsigned long
get_hash_value(unsigned char *str)
{
    unsigned long hash = 5381;
    int c;

    while (c = *str++)
        hash = ((hash << 5) + hash) + c;

        return hash;
}

void * read_search_and_count()
{
    details         *det = &g_details;
    unsigned  long  word_hash;
    int32_t         last_seq = det->seq;
    char            words[MAX_CHAR_PER_WORD];
    int             i = 0, j = 0, idx = -1;
    struct          stat attr;
    unsigned long    lmtime = 1, ret = 0;
    unsigned long   nbytes = 0;

    det->fmain = fopen(det->fname_main, "r");
    if (det->fmain == NULL) {
        printf("Error, file open failed\n");
        exit(1);
    }
    stat(det->fname_main, &attr);
    lmtime = attr.st_mtime;
    
    prctl(PR_SET_NAME, "rd_search_thrd", 0, 0, 0);
    while (1) {
        char ch;

        ch = fgetc(det->fmain);
        /* as file size is infinite,
         * it may not hit the following condition at all*/
        if (ch == EOF) {
            fclose(det->fmain);
            det->fmain = NULL;
#if 0
            int j = 0;
            for (j = 0; j < det->total_wcount; j++) {
                printf("the word %s, its hash value %lu\n",
                        det->words[j], det->hash_value[j]);
            }
#endif /*if 0*/
            display_stats();
            break;
        }
        /*store the offset*/
        nbytes++;
        if (ch != ' ' && ch != '\n') {
            words[i++] = ch; 
            //printf("Error, char read  %c\n", ch);
            continue;
        } else {
            words[i] = '\0';
            i = 0;
        }
        /* search the word in the list of words read from the
         * second file */
        pthread_mutex_lock(&det->rw_lock);
        word_hash = get_hash_value(words);
        idx = hash_binary_search(get_hash_value(words), 0,
                    det->total_wcount - 1);
        if (idx != -1) {
            stat(det->fname_main, &attr);
            lmtime = attr.st_mtime;
            det->wcount[idx]++;
        }
        ret = is_file_modified(1, lmtime);
        if (0 < ret) {
            fclose(det->fmain);
            det->fmain = fopen(det->fname_main, "r");
            if (!det->fmain) {
                pthread_mutex_unlock(&det->rw_lock);
                return;
            }
            lmtime = ret;
#ifdef DEBUG 
            printf("FILE REOPNED\n");
            fseek(det->fmain, nbytes, SEEK_SET);
#endif  /*DEBUG*/
        }
        /* release the lock*/
        pthread_mutex_unlock(&det->rw_lock);
        usleep(10000);
    }
    return;
}

/* sort the hash values and order the strings according
 * to the change in the position of its respective hash value */
void
rearrange_string_hash_elemets ()
{
    unsigned long   temp;
    char            str_temp[MAX_CHAR_PER_WORD];
    int             i = 0, j = 0;
    struct          details *det = &g_details;

#ifdef DEBUG 
    printf("BEFOER SORTING:\n");
    for (i = 0; i < det->total_wcount; i++) {
        printf("WORD %s, hash_value %lu\n", det->words[i], det->hash_value[i]);
    }
#endif  /*DEBUG*/

    for (i = 0; i < det->total_wcount - 1; i++) {
        for (j = 0; j < det->total_wcount-i-1; j++) {
            if (det->hash_value[j] > det->hash_value[j+1]) {
                /*swap hash values*/
                temp = det->hash_value[j];
                det->hash_value[j] = det->hash_value[j+1];
                det->hash_value[j+1] = temp;

                /*swap the strings at the positions*/
                strcpy(str_temp, det->words[j]);
                strcpy(det->words[j], det->words[j+1]);
                strcpy(det->words[j+1], str_temp);
            }
            
        }
    }
#ifdef DEBUG 
    printf("AFTER SORTING:\n");
    for (i = 0; i < det->total_wcount; i++) {
        printf("WORD %s, hash_value %lu\n", det->words[i], det->hash_value[i]);
    }
#endif /*DEBUG*/    

}

int
hash_binary_search (unsigned long hash_eq, int low, int high)
{
    details     *det = &g_details;
    uint32_t    mid = (low + high)/2;

    if (low > high) {
        /* word not present*/
        return -1;
    }

    //printf("Values of mid %d\n", mid);
    //printf("HASH value %lu, hash from array %lu\n",
     //   hash_eq, det->hash_value[mid]);
    if (hash_eq == det->hash_value[mid]) {
        return mid;
    }

    if (hash_eq < det->hash_value[mid]) {
        return hash_binary_search(hash_eq, low, mid-1);
    } else {
        return hash_binary_search(hash_eq, mid+1, high);
    }
}

void reset_counter()
{
    details *det    = &g_details;
    int     i       = 0;

    pthread_mutex_lock(&det->rw_lock);
    for (i = 0; i < det->total_wcount; i++)  {
        det->wcount[i] = 0;
    }
    pthread_mutex_unlock(&det->rw_lock);
    return;
}

uint8_t is_duplicate(char *word)
{
    details         *det    = &g_details;
    unsigned long   hvalue  = get_hash_value(word);
    int             i       = 0;

    /* cannot use binary search here as the array is not sorted
     * yet, hence going linear search*/
#if 0
    if (-1 ==  hash_binary_search(get_hash_value(word),
                            0, det->total_wcount)) {
        printf("NOT FOUND\n");
        return 0;
    }
#endif /*if 0*/

    for (i = 0; i < det->total_wcount; i++) {
        if (det->hash_value[i] == hvalue) {
            return 1;
        }
    }
    return 0;
}

/*  return 0 if not modified else returns
 *  last modfied timestamp */
unsigned long
is_file_modified(uint8_t is_main_file, unsigned long lmtime)
{
    details *det = &g_details;
    struct stat attr;

    if (is_main_file) {
        stat(det->fname_main, &attr);
        if (attr.st_mtime > lmtime) {
            return attr.st_mtime;
        } else {
            return 0;
        }
    }
}
