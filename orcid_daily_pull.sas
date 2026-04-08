/*==============================================================*/
/* PROGRAM 1: DAILY ORCID PULL                                  */
/* Saves normal .sas7bdat files for later use by Program 2      */
/*==============================================================*/

options spool source2 mprint mlogic symbolgen;

/*--------------------------------------------------------------*/
/* INCLUDE ORCID MACRO                                          */
/*--------------------------------------------------------------*/
%include "/prj/dcp/restricted/cpfp_eval/progs/orcidtesting/orcidtesting.sas";

/*--------------------------------------------------------------*/
/* USER SETTINGS                                                */
/*--------------------------------------------------------------*/
%let excel_file   = /prj/dcp/restricted/cpfp_eval/progs/orcidtesting/source/Alumni List with Orcids.xlsx;
%let excel_sheet  = Sheet1;
%let sas_file     = /prj/dcp/restricted/cpfp_eval/progs/orcidtesting/source/cpfp_master_fellow_list_20260325.sas7bdat;

/* permanent save location for .sas7bdat files */
libname savelib "/prj/dcp/restricted/cpfp_eval/progs/orcidtesting/unzip";

/* working outputs */
%let final_out  = work.all_orcid_publications;
%let person_out = work.orcid_person_summary;

/* saved .sas7bdat outputs */
%let pubs_file   = savelib.all_orcid_publications;
%let person_file = savelib.orcid_person_summary;

/* optional Excel outputs */
%let pub_xlsx    = all_orcid_publications.xlsx;
%let person_xlsx = orcid_person_summary.xlsx;

/*--------------------------------------------------------------*/
/* STEP 1: Read Excel                                           */
/*--------------------------------------------------------------*/
proc import
  datafile="&excel_file"
  out=work.orcid_xlsx
  dbms=xlsx
  replace;
  sheet="&excel_sheet";
  getnames=yes;
run;

data work.orcid_xlsx2;
  length cpfp_id $50 orcid $25;
  set work.orcid_xlsx;

  cpfp_id = strip(vvalue(cpfp_id));
  orcid   = strip(vvalue(orcid));

  if not missing(cpfp_id) and not missing(orcid);

  keep cpfp_id orcid;
run;

proc sort data=work.orcid_xlsx2 nodupkey;
  by cpfp_id;
run;

/*--------------------------------------------------------------*/
/* STEP 2: Read SAS dataset                                     */
/*--------------------------------------------------------------*/
data work.name_file;
  set "&sas_file";
run;

data work.name_file2;
  length cpfp_id $50 first_name $200 last_name $200;
  set work.name_file;

  cpfp_id    = strip(vvalue(cpfp_id));
  first_name = strip(vvalue(first_name));
  last_name  = strip(vvalue(last_name));

  if not missing(cpfp_id);

  keep cpfp_id first_name last_name;
run;

proc sort data=work.name_file2 nodupkey;
  by cpfp_id;
run;

/*--------------------------------------------------------------*/
/* STEP 3: Merge names + ORCID                                  */
/*--------------------------------------------------------------*/
data work.people_for_orcid;
  merge
    work.name_file2  (in=in_names)
    work.orcid_xlsx2 (in=in_orcid)
  ;
  by cpfp_id;

  if in_names and in_orcid;
run;

proc sort data=work.people_for_orcid nodupkey;
  by cpfp_id;
run;

/*--------------------------------------------------------------*/
/* STEP 4: Initialize publication output                        */
/*--------------------------------------------------------------*/
data &final_out;
  length
    cpfp_id $50
    first_name $200
    last_name $200
    orcid $25
    put_code $32
    title $1000
    journal $500
    pub_year $4
    pub_month $2
    pub_day $2
    url $1000
    doi $500
    pmid $32
  ;
  stop;
run;

/*--------------------------------------------------------------*/
/* STEP 5: Pull one person's ORCID works                        */
/*--------------------------------------------------------------*/
%macro one_person(cpfp_id=, orcid=);

  %local clean_cpfp clean_orcid outmem outds_safe;

  /* cleaned values only for temp dataset names / API arg */
  %let clean_cpfp = %sysfunc(translate(%superq(cpfp_id),_,-));
  %let clean_cpfp = %sysfunc(compress(&clean_cpfp,,kw));

  %let clean_orcid = %sysfunc(compress(%superq(orcid),0123456789-,k));

  %let outmem = pubs_&clean_cpfp;
  %let outds_safe = work.&outmem;

  %orcid_publications_simple(
    orcid=&clean_orcid,
    outds=&outds_safe
  );

  %if %sysfunc(exist(&outds_safe)) %then %do;

    data _pubs_tagged;
      length cpfp_id $50 orcid $25;
      set &outds_safe;

      cpfp_id = "&cpfp_id";
      orcid   = "&clean_orcid";
    run;

    proc append base=&final_out data=_pubs_tagged force;
    run;

    proc datasets lib=work nolist;
      delete _pubs_tagged &outmem &outmem._pmids;
    quit;

  %end;

%mend;

/*--------------------------------------------------------------*/
/* STEP 6: Create macro vars                                    */
/*--------------------------------------------------------------*/
data _null_;
  set work.people_for_orcid end=eof;

  call symputx(cats('cpfp_id', _n_), strip(cpfp_id), 'l');
  call symputx(cats('orcid', _n_), strip(orcid), 'l');

  if eof then call symputx('n_people', _n_, 'l');
run;

/*--------------------------------------------------------------*/
/* STEP 7: Run all                                              */
/*--------------------------------------------------------------*/
%macro run_all_people;

  %local i;

  %do i=1 %to &n_people;

    %one_person(
      cpfp_id=&&cpfp_id&i,
      orcid=&&orcid&i
    );

  %end;

%mend;

%run_all_people;

/*--------------------------------------------------------------*/
/* STEP 8: Merge names onto publication dataset                 */
/*--------------------------------------------------------------*/
proc sort data=&final_out;
  by cpfp_id;
run;

proc sort data=work.people_for_orcid
          out=work.name_lookup(keep=cpfp_id first_name last_name orcid)
          nodupkey;
  by cpfp_id;
run;

data &final_out;
  merge
    &final_out (in=a)
    work.name_lookup(rename=(
      first_name=lookup_first_name
      last_name =lookup_last_name
    ) in=b)
  ;
  by cpfp_id;

  length first_name last_name hold_first_name hold_last_name $200;
  retain hold_first_name hold_last_name;

  if first.cpfp_id then call missing(hold_first_name, hold_last_name);

  if b then do;
    hold_first_name = lookup_first_name;
    hold_last_name  = lookup_last_name;
  end;

  first_name = hold_first_name;
  last_name  = hold_last_name;

  if a;

  drop lookup_first_name lookup_last_name hold_first_name hold_last_name;
run;

/*--------------------------------------------------------------*/
/* STEP 9: Build person-level summary                           */
/*--------------------------------------------------------------*/
proc sort data=&final_out
          out=work.pub_count_base(keep=cpfp_id put_code)
          nodupkey;
  by cpfp_id put_code;
run;

data work.pub_counts;
  set work.pub_count_base;
  by cpfp_id;
  retain publication_count 0;

  if first.cpfp_id then publication_count = 0;
  publication_count + 1;

  if last.cpfp_id then output;

  keep cpfp_id publication_count;
run;

data &person_out;
  merge
    work.name_lookup (in=a)
    work.pub_counts  (in=b)
  ;
  by cpfp_id;

  if a;
  if not b then publication_count = 0;
run;

/*--------------------------------------------------------------*/
/* STEP 10: Save .sas7bdat files                                */
/*--------------------------------------------------------------*/
data &pubs_file(compress=binary);
  set &final_out;
run;

data &person_file(compress=binary);
  set &person_out;
run;

/*--------------------------------------------------------------*/
/* STEP 11: Optional Excel exports                              */
/*--------------------------------------------------------------*/
proc export
  data=&final_out
  outfile="&pub_xlsx"
  dbms=xlsx
  replace;
  sheet="publications";
run;

proc export
  data=&person_out
  outfile="&person_xlsx"
  dbms=xlsx
  replace;
  sheet="person_summary";
run;

/*--------------------------------------------------------------*/
/* STEP 12: Review                                              */
/*--------------------------------------------------------------*/
title "Publication dataset sample";
proc print data=&final_out(obs=20);
run;

title "Person summary sample";
proc print data=&person_out(obs=20);
run;
title;