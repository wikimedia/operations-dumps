#include <getopt.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <errno.h>
#include <sys/ioctl.h> 
#include <string.h>
#include <unistd.h>
#include <zlib.h>
#include "uthash.h"

struct list_entry {
  void *value;
  struct list_entry *prev, *next;
};

struct list {
  struct list_entry *first, *last;
};

struct project {
  char *name;
  int count;
};

struct media_hentry {
  char *media_name; /* key */
  struct list projects;
  UT_hash_handle hh;
};

struct project_hentry {
  char *name; /* key = project name */
  int count;  /* count of media in this project overlapping with project in chart_hentry name */
  UT_hash_handle hh;
};

struct chart_hentry {
  char *name; /* key = project name */
  int count;  /* total media in the project */
  struct project_hentry *proj;
  UT_hash_handle hh;
};

struct list_entry *find_value_in_list(char *value, struct list *list_to_check)  {
  struct list_entry *elt;

  elt = list_to_check->first;
  while (elt) {
    if (elt->value && !strcmp((char *)(elt->value), value))
      return(elt);
    elt = elt->next;
  }
  return(NULL);
}

struct list_entry *find_value_in_proj_list(char *value, struct list *list_to_check)  {
  struct list_entry *elt;

  elt = list_to_check->first;
  while (elt) {
    if (elt->value && !strcmp(((struct project *)(elt->value))->name, value))
      return(elt);
    elt = elt->next;
  }
  return(NULL);
}

int project_list_contains(struct list *pnames, struct list *projects) {
  /* see if list of projects (struct project *) contains all project names in pnames */
  struct list_entry *pname, *p;
  int found;

  p = projects->first;
  while (p) {
    found = 0;
    pname = pnames->first;
    while (pname) {
      if (!strcmp((char *)(pname->value),((struct project *)(p->value))->name)) {
	found++;
	break;
      }
      pname = pname->next;
    }
    if (!found)
      return(0);
    p = p->next;
  }
  return(1);
}

char *list_to_string(struct list *list_to_format) {
  char *result;
  struct list_entry *elt;
  int length;

  elt = list_to_format? list_to_format->first : NULL;
  if (!elt || !elt->value)
    return("");

  result = strdup((char *)(elt->value));
  elt = elt->next;

  while (elt) {
    if (elt->value) {
      length = strlen(result);
      result = (char *)realloc(result, length + strlen((char *)(elt->value)) + 2);
      result[length] = ' ';
      strcpy(result + length + 1, (char *)(elt->value));
    }
    elt = elt->next;
  }
  return(result);
}

char *proj_list_to_string(struct list *list_to_format) {
  char *result;
  struct list_entry *elt;
  int length;

  elt = list_to_format? list_to_format->first : NULL;
  if (!elt || !elt->value)
    return("");

  result = strdup(((struct project *)(elt->value))->name);
  elt = elt->next;

  while (elt) {
    if (elt->value) {
      length = strlen(result);
      result = (char *)realloc(result, length + strlen(((struct project *)(elt->value))->name) + 2);
      result[length] = ' ';
      strcpy(result + length + 1, ((struct project *)(elt->value))->name);
    }
    elt = elt->next;
  }
  return(result);
}

struct chart_hentry *chart_update(struct chart_hentry *chart_table, struct list *projects, struct list *top_n, char **projs_to_add) {
  int i=0, j, last = 0;
  struct list_entry *p, *top_n_ptr;
  struct project_hentry *proj, *found = NULL;
  struct chart_hentry *row_found = NULL, *cr = NULL;
  /*  struct chart_hentry *cr=NULL;*/

  p = projects->first;
  while (p) {
    if (find_value_in_proj_list(p->value, top_n)) {
      projs_to_add[i] = (char *)(p->value);
      i++; 
    }
    p = p->next;
  }
  projs_to_add[i] = NULL;
  last = i;

  /* ok now we have an array of the proj names this media file is in, that are also in top n with NULL at the end of the array */
  for (i=0; i<last; i++) {
    /* create the hash entry which will hold proj name/media file count 
       for each project which has media files in common with this project */
    HASH_FIND_STR(chart_table, (char *)(projs_to_add[i]), row_found);
    if (!row_found) {
      if ((cr = (struct chart_hentry *)malloc(sizeof(struct chart_hentry))) == NULL) {
	fprintf(stderr,"failed to allocate memory\n");
	exit(1);
      }
      cr->name = (char *)(projs_to_add[i]);
      if ((top_n_ptr = find_value_in_proj_list(cr->name, top_n)) != NULL) {
	cr->count = ((struct project_hentry *)top_n_ptr->value)->count;
      }
      cr->proj = NULL;
      HASH_ADD_KEYPTR(hh, chart_table, projs_to_add[i], strlen(projs_to_add[i]), cr);
      row_found = cr;
    }
    for (j=0; j<last; j++) {
      if (j!=i) {
	/* up the count for the number of files common to projs_to_add[i] and projs_to_add[j] */
	HASH_FIND_STR(row_found->proj, (char *)(projs_to_add[j]), found);
	if (!found) {
	  if ((proj = (struct project_hentry *)malloc(sizeof(struct project_hentry))) == NULL) {
	    fprintf(stderr,"failed to allocate memory\n");
	    exit(1);
	  }
	  proj->name = (char *)(projs_to_add[j]);
	  proj->count = 1;
	  HASH_ADD_KEYPTR(hh, row_found->proj, projs_to_add[j], strlen(projs_to_add[j]), proj);
	}
	else {
	  found->count++;
	}
      }
    }
  }
  return(chart_table);
}

int count_list_entries(struct list *l) {
  int count;
  struct list_entry *elt;

  elt = l? l->first : NULL;
  if (!elt)
    return(0);

  count = 1;
  while ((elt = elt->next) != NULL)
    count++;

  return(count);
}


struct project_hentry *tally_list(struct list *projects, struct project_hentry *totals) {
  struct list_entry *elt;
  struct project_hentry *found, *t;

  if (!projects)
    return totals;
  elt = projects->first;
  while (elt) {
    if (elt->value) {
      HASH_FIND_STR(totals, (char *)(elt->value), found);
      if (!found) {
	if ((t = malloc(sizeof(struct project_hentry))) == NULL) {
	  fprintf(stderr,"failed to allocate memory\n");
	  exit(1);
	}
	t->name = (char *)(elt->value);
	t->count = 1;
	HASH_ADD_KEYPTR(hh, totals, t->name, strlen(t->name), t);
      }
      else
	found->count++;
    }
    elt = elt->next;
  }
  return totals;
}

void sort_into_position(struct list *top_n, char *proj_name, int count, int chart) {
  struct list_entry *elt, *prev_elt, *spare, *for_use;
  int length = 0;

  prev_elt = NULL;
  elt = top_n ? top_n->first : NULL;

  while (elt) {
    if (count > ((struct project_hentry *)(elt->value))->count)
      break;
    prev_elt = elt;
    length++;
    elt = elt->next;
  }
  if (length < chart) {
    /* insert new elt and drop the last elt off the end of the list if list is too long */
    if (count_list_entries(top_n) == chart) {
      /* pull the last item out of the list for use to insert */
      if (elt == top_n->last)
	elt = NULL; /* in fact not inserting before an elt, as the elt will get bumped off */

      /* pull off the extra item */
      spare = top_n->last;

      /* shorten list */
      top_n->last = spare->prev;
      spare->prev->next = NULL; 

      /* reset the extra item for use */
      for_use = spare;
      for_use->next = NULL;
      for_use->prev = NULL;
    }
    else {
      /* no last item spare so we malloc one */
      for_use = (struct list_entry *)malloc(sizeof(struct list_entry));
      for_use->next = NULL;
      for_use->prev = NULL;
      for_use->value = (struct project *)malloc(sizeof(struct project));
    }
    ((struct project *)(for_use->value))->name = proj_name;
    ((struct project *)(for_use->value))->count = count;

    /* inserting at the head of the list? */
    if (prev_elt) {
      for_use->prev = prev_elt;
      prev_elt->next = for_use;
    }
    else {
      top_n->first = for_use;
    }
    /* inserting at the tail of the list? */
    if (elt) {
      elt->prev = for_use;
      for_use->next = elt;
    }
    else {
      top_n->last = for_use;
    }
  }
  return;
}

void *open_file(int compressed, char *filename) {
  gzFile *gfd = NULL;
  FILE *fd;

  if (compressed) {
    if ((gfd = gzopen(filename, "r")) == NULL) {
      fprintf(stderr,"failed to open file %s\n", filename);
      exit(1);
    }
    return((void *) gfd);
  }
  else {
    if ((fd = fopen(filename, "r")) == NULL) {
      fprintf(stderr,"failed to open file %s\n", filename);
      exit(1);
    }
    return((void *) fd);
  }
}

char *read_buffer(int compressed, void *fd, char *buffer, int size) {
  if (compressed)
    return(gzgets((gzFile *) fd, buffer, size));
  else
    return(fgets(buffer, size, (FILE *) fd));
}

void close_file(int compressed, void *fd) {
  if (compressed)
    gzclose((gzFile *)fd);
  else
    fclose((FILE *)fd);
}

char *proj_name_from_filename(char *filename) {
  /* assume filename has form [blah.../]ruwiki-20120801-remote-wikiqueries... */
  char *start_ptr, *end_ptr;

  start_ptr = strrchr(filename, '/');
  if (!start_ptr)
    start_ptr = filename;
  else
    start_ptr++;

  end_ptr = strchr(start_ptr, '-');
  if (end_ptr)
    return(strndup(start_ptr, end_ptr - start_ptr));
  else
    return("unknown");
}

char *get_media_name(char *line) {
  /* assume line has format Black-Sea-NASA.jpg^I20051028122833 */
  char *sep_ptr;

  sep_ptr = strchr(line, '\t');
  if (sep_ptr) {
    return(strndup(line,sep_ptr - line));
  }
  else {
    fprintf(stderr,"weird media line %s\n", line);
    return("unknown");
  }
}

void usage(char *whoami, char *message) {
  if (message)
    fprintf(stderr, "Error: %s\n\n", message);

  fprintf(stderr,"usage: %s [--atleast] [--chart] [--intersect] [--matches] [--project] [--tally] [--verbose] filename...\n", whoami);
  fprintf(stderr,"\n");
  fprintf(stderr,"options:\n");
  fprintf(stderr,"  --atleast   (-a): show only media files included in at least this many projects\n");
  fprintf(stderr,"  --chart     (-c): show chart for this many top projects\n");
  fprintf(stderr,"  --intersect (-i): show count of media in use on all projects for this many top projects\n");
  fprintf(stderr,"  --matches   (-m): show only media files included in exactly this many projects\n");
  fprintf(stderr,"  --project   (-p): show only media files included in this project\n");
  fprintf(stderr,"\n");
  fprintf(stderr,"flags:\n");
  fprintf(stderr,"  --stats   (-s): show statistics about each media file\n");
  fprintf(stderr,"  --tally   (-t): show a tally of how many files are included in each project\n");
  fprintf(stderr,"  --verbose (-v): display extra messages describing what the program is doing\n");
  fprintf(stderr,"\n");
  fprintf(stderr,"example usage:\n");
  fprintf(stderr,"%s --matches 3 --project elwiki --tally *-20120801-remote-wikiqueries.gz\n", whoami);
  exit(1);
}

/* 
   Script to read multiple lists of media files in use on a given wiki project,
   hosted remotely, and produce statistics about the inclusion of those files
   across the projects.

   First I wrote this script in python / but it ran too slow for me
   Soon I found the answer / Write in C
*/

int main(int argc, char **argv) {

  struct media_hentry *media_usage = NULL, *found, *m;
  struct project_hentry *project_totals = NULL, *t;
  struct list_entry *proj_info = NULL;

  struct list top_n;
  struct list inters_top_n;

  struct chart_hentry *chart_table = NULL, *c_row = NULL;
  struct project_hentry *c_proj = NULL;

  struct option optvalues[] = {
    {"atleast", 1, 0, 'a'},
    {"chart", 1, 0, 'c'},
    {"intersect", 1, 0, 'i'},
    {"matches", 1, 0, 'm'},
    {"project", 1, 0, 'p'},
    {"stats", 0, 0, 's'},
    {"tally", 0, 0, 't'},
    {"verbose", 0, 0, 'v'},
    {NULL, 0, NULL, 0}
  };

  char **projs_to_add = NULL;
  char buffer[280]; /* page titles must be 256 bytes or less... for now. then add timestamp, tab, newline etc. */
  char *pname, *mname;
  char *project = NULL;
  char *formatted;

  void *fd = NULL;

  int matches = 0, verbose = 0, tally = 0, stats = 0, atleast = 0, chart = 0, intersection = 0;
  int i, l, count, media_count, optc;
  int compressed = 0, optindex=0;

  while (1) {
    optc=getopt_long_only(argc,argv,"a:c:i:m:p:stv", optvalues, &optindex);
    if (optc == 'a') {
      if (!optarg || !(isdigit(optarg[0]))) usage(argv[0],"atleast must be a positive integer");
      atleast = atoi(optarg);
    }
    else if (optc == 'c') {
      if (!optarg || !(isdigit(optarg[0]))) usage(argv[0],"chart must be a positive integer");
      chart = atoi(optarg);
    }
    else if (optc == 'i') {
      if (!optarg || !(isdigit(optarg[0]))) usage(argv[0],"intersect must be a positive integer");
      intersection = atoi(optarg);
    }
    else if (optc == 'm') {
      if (!optarg || !(isdigit(optarg[0]))) usage(argv[0],"matches must be a positive integer");
      matches = atoi(optarg);
    }
    else if (optc == 'p') {
      if (!optarg) {
	usage(argv[0],"project must be a project name such as elwikisource");
      }
      project = optarg;
    }
    else if (optc == 's') {
      stats++;
    }
    else if (optc == 't') {
      tally++;
    }
    else if (optc == 'v') {
      verbose++;
    }
    else if (optc==-1) break;
    else usage(argv[0],"Unknown option or other error\n");
  }

  if (! (argc - optind))
    usage(argv[0],"missing filenames");
  if (verbose)
    fprintf(stdout,"processing files and collecting initial data\n");

  for (i = optind; i < argc; i++) {
    l = strlen(argv[i]);
    if ( l > 3 && !strcmp(".gz", (char *)(argv[i]+l-3) ))
      compressed++;

    fd = open_file(compressed, argv[i]);
    if (verbose)
      fprintf(stdout, "processing file %s\n", argv[i]);
    pname = proj_name_from_filename((argv[i]));
    while (read_buffer(compressed, fd, buffer, sizeof(buffer)) != NULL) {
      if (strlen(buffer) == sizeof(buffer)-1 && buffer[sizeof(buffer)-2] != '\n') {
	fprintf(stderr,"title too long, expected less than %ld, got %s\n", sizeof(buffer), buffer);
	exit(1);
      }
      buffer[strlen(buffer)-1] = '\0';
      mname = get_media_name(buffer);
      HASH_FIND_STR(media_usage, mname, found);
      proj_info = malloc(sizeof(struct list_entry));
      if (!proj_info) {
	fprintf(stderr,"failed to allocate memory\n");
	exit(1);
      }
      proj_info->value = pname;
      proj_info->next = NULL;

      if (!found) {
	if ((m = malloc(sizeof(struct media_hentry))) == NULL) {
	  fprintf(stderr,"failed to allocate memory\n");
	  exit(1);
	}
	m->media_name = mname;
	proj_info->prev = NULL;
	m->projects.first = proj_info;
	m->projects.last = proj_info;
	HASH_ADD_KEYPTR(hh, media_usage, m->media_name, strlen(m->media_name), m);
      }
      else {
	proj_info->prev = found->projects.last;
	found->projects.last->next = proj_info;
	found->projects.last = proj_info;
      }
    }
    close_file(compressed, fd);
  }
  if (stats) {
    if (verbose)
      fprintf(stdout,"doing display of stats\n");
    for (m=media_usage; m != NULL; m=m->hh.next) {
      count = count_list_entries(&(m->projects));
      if ((matches && count == matches) ||
	  (atleast && count >= atleast) ||
	  (!matches && !atleast)) {
	if ((project && find_value_in_list(project, &(m->projects))) || !project) {
	  /* seriously? malloc and free for every filename? bleah  FIXME */
	  formatted = list_to_string(&(m->projects));
	  fprintf(stdout, "%d %s %s\n", count, m->media_name, formatted);
	  free(formatted);
	}
      }
    }
  }
  if (tally | chart | intersection ) {
    if (verbose)
      fprintf(stdout,"gathering stats for tally/chart/intersect\n");
    /* need to get these numbers for the chart as well, so we can figure out which
       are the top projects */
    for (m=media_usage; m != NULL; m=m->hh.next) {
      count = count_list_entries(&(m->projects));
      if ((matches && count == matches) ||
	  (atleast && count >= atleast) ||
	  (!matches && !atleast)) {
	  if ((project && find_value_in_list(project, &(m->projects))) || !project)
	    project_totals = tally_list(&(m->projects), project_totals);
      }
    }
    if (tally) {
      if (verbose)
	fprintf(stdout,"writing tally\n");
      for (t=project_totals; t != NULL; t=t->hh.next) {
	fprintf(stdout, "%d %s\n", t->count, t->name);
      }
    }
  }
  if (chart) {
    /* find the projects in project_totals which are the top n for chart */
    top_n.first = NULL;
    top_n.last = NULL;
    if (verbose)
      fprintf(stdout,"gathering stats for chart, phase 1\n");
    for (t=project_totals; t != NULL; t=t->hh.next) {
      sort_into_position(&top_n, t->name, t->count, chart);
    }    
  }

  if (intersection) {
    /* find the projects in project_totals which are the top n for intersection */
    inters_top_n.first = NULL;
    inters_top_n.last = NULL;
    if (verbose)
      fprintf(stdout,"gathering stats for intersect, phase 1\n");
    for (t=project_totals; t != NULL; t=t->hh.next) {
      sort_into_position(&inters_top_n, t->name, t->count, intersection);
    }    
  }

  if (chart) {
    if (verbose)
      fprintf(stdout,"gathering stats for chart, phase 2\n");

    projs_to_add = (char **)malloc((chart+1)*sizeof(char *));

    for (m=media_usage; m != NULL; m=m->hh.next) {
      count = count_list_entries(&(m->projects));
      if ((matches && count == matches) ||
	  (atleast && count >= atleast) ||
	  (!matches && !atleast)) {
	if ((project && find_value_in_proj_list(project, &(m->projects))) || !project)
	  chart_table = chart_update(chart_table, &(m->projects), &top_n, projs_to_add);
      }
    }

    if (verbose)
      fprintf(stdout,"displaying stats for chart\n");

    for (c_row = chart_table; c_row != NULL; c_row = c_row->hh.next) {
      fprintf(stdout, "%s(%d): ", c_row->name, c_row->count);
      for (c_proj = c_row->proj; c_proj != NULL; c_proj = c_proj->hh.next) {
	fprintf(stdout, "%d %s  ", c_proj->count, c_proj->name);
      }
      fprintf(stdout,"\n");
    }
  }

  if (intersection) {
    if (verbose)
      fprintf(stdout,"gathering stats for intersec, phase 2\n");

    media_count = 0;
    for (m=media_usage; m != NULL; m=m->hh.next) {
      count = count_list_entries(&(m->projects));
      if ((matches && count == matches) ||
	  (atleast && count >= atleast) ||
	  (!matches && !atleast)) {
	  if ((project && find_value_in_list(project, &(m->projects))) || !project)
	    /* see if this media file is in all the top n projects (n given by the 
	       arg to the intersect option) */
	    if (project_list_contains(&(m->projects), &inters_top_n)) {
	      media_count ++;
	    }
      }
    }

    if (verbose)
      fprintf(stdout,"displaying stats for intersec\n");
    fprintf(stdout,"Media contained in all of the top %d projects: %d\n", intersection, media_count);
  }

  exit(0);
}
