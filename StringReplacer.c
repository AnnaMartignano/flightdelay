#include <stdio.h>
#include <stdlib.h>
#include <string.h>


#define MAX_LEN 10240
#define MIN_LEN 1024

char * in_buff;
char * out_buff;

char strin[MIN_LEN+1];
char strout[MIN_LEN+1];

char *replace(char *str, char *orig, char *rep)
{
  static char buffer[MAX_LEN +1];
  char *p;

  if(!(p = strstr(str, orig)))  // Is 'orig' even in 'str'?
    return str;

  strncpy(buffer, str, p-str); // Copy characters from 'str' start to 'orig' st$
  buffer[p-str] = '\0';

  sprintf(buffer+(p-str), "%s%s", rep, p+strlen(orig));

  return buffer;
}

void convert(FILE *fr, FILE *fw) {
  size_t len;
  while (getline(&in_buff, &len, fr) != -1) {
    strncpy(out_buff, in_buff, MAX_LEN);
    while (strstr(out_buff, strin) != (char *) NULL) {
       strncpy(out_buff, replace(out_buff, strin, strout), MAX_LEN);
    }
    fprintf(fw, "%s", out_buff);
  }
}

int main(int argc, char *argv[]) {

  FILE *fr, *fw;

  if (argc != 5) {
	fprintf(stderr, "Wrong command line!\n");
	fprintf(stderr, "Syntax: %s <filein> <fileout> <strin> <strout>\n", argv[0]);
	return -1;
  }
  
  if ((fr = fopen(argv[1], "r")) == (FILE *) NULL) {
	fprintf(stderr, "Cannot open input file \"%s\".\n", argv[1]);
	return -1;
  }
  
  if ((fw = fopen(argv[2], "w")) == (FILE *) NULL) {
	fprintf(stderr, "Cannot open output file \"%s\".\n", argv[2]);
        fclose(fr);
	return -1;
  }

  if ((in_buff = (char *) malloc(MAX_LEN+1)) == (char *) NULL) {
	fprintf(stderr, "Cannot allocate input buffer.\n");
        fclose(fr);
        fclose(fw);
	return -1;
  }
  
  if ((out_buff = (char *) malloc(MAX_LEN+1)) == (char *) NULL) {
	fprintf(stderr, "Cannot allocate output buffer.\n");
        fclose(fr);
        fclose(fw);
	return -1;
  }

  strncpy(strin, argv[3], MIN_LEN);
  strncpy(strout, argv[4], MIN_LEN);

  convert(fr, fw);

  free(in_buff);
  free(out_buff);
  
  fclose(fr);
  fclose(fw);
  
  return 0;
}




