/*==============================================================*/
/* PROGRAM 2: REPORTING FROM SAVED SAS7BDAT FILES               */
/*==============================================================*/

options spool source2 mprint mlogic symbolgen;

/*--------------------------------------------------------------*/
/* USER SETTINGS                                                */
/*--------------------------------------------------------------*/
%let excel_file   = /prj/dcp/restricted/cpfp_eval/progs/orcidtesting/source/cpfp_merged_orcid.xlsx;
%let excel_sheet  = cpfp_merged;
%let sas_file     = /prj/dcp/restricted/cpfp_eval/progs/orcidtesting/source/cpfp_master_fellow_list_20260325.sas7bdat;

/* location where Program 1 saved datasets */
libname savelib "/prj/dcp/restricted/cpfp_eval/progs/orcidtesting/";

/* saved datasets from Program 1 */
%let pubs_in   = work.all_orcid_publications;
%let person_in = work.orcid_person_summary;

/* reporting outputs */
%let person_full    = work.person_full_report;
%let pub_full       = work.publication_full_report;
%let no_orcid       = work.no_orcid_report;
%let mismatch_out   = work.name_mismatch_report;

/* Excel outputs */
%let person_xlsx    = person_full_report.xlsx;
%let pub_xlsx       = publication_full_report.xlsx;
%let no_orcid_xlsx  = no_orcid_report.xlsx;
%let mismatch_xlsx  = name_mismatch_report.xlsx;

/*--------------------------------------------------------------*/
/* STEP 1: READ SAVED SAS DATASETS                              */
/*--------------------------------------------------------------*/
data &pubs_in;
  set savelib.all_orcid_publications;
run;

data &person_in;
  set savelib.orcid_person_summary;
run;

/*--------------------------------------------------------------*/
/* STEP 2: READ ORIGINAL EXCEL FILE                             */
/*--------------------------------------------------------------*/
proc import
  datafile="&excel_file"
  out=work.orcid_xlsx
  dbms=xlsx
  replace;
  sheet="&excel_sheet";
  getnames=yes;
run;

/* keep the raw Excel name variable if present */
data work.orcid_xlsx2;
  length cpfp_id $50 orcid $25 excel_name_raw $400 excel_first_name $200 excel_last_name $200;
  set work.orcid_xlsx;

  cpfp_id = strip(vvalue(cpfp_id));
  orcid   = strip(vvalue(orcid));

  /* imported Excel variable from earlier logs */
  excel_name_raw = coalescec(
                     strip(vvalue(NAME__Last__First_)),
                     strip(vvalue(name_alumni_list)),
                     ''
                   );

  /* parse "Last, First" if present */
  if index(excel_name_raw, ',') then do;
    excel_last_name  = strip(scan(excel_name_raw, 1, ','));
    excel_first_name = strip(scan(excel_name_raw, 2, ','));
  end;
  else do;
    excel_last_name  = '';
    excel_first_name = '';
  end;

  if not missing(cpfp_id) and not missing(orcid);

  keep cpfp_id orcid excel_name_raw excel_first_name excel_last_name;
run;

proc sort data=work.orcid_xlsx2 nodupkey;
  by cpfp_id;
run;

/*--------------------------------------------------------------*/
/* STEP 3: READ ORIGINAL SAS PERSON FILE                        */
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

  keep cpfp_id first_name last_name;
run;

proc sort data=work.name_file2 nodupkey;
  by cpfp_id;
run;

/*--------------------------------------------------------------*/
/* STEP 4: BUILD PERSON LOOKUP                                  */
/*--------------------------------------------------------------*/
data work.people_lookup;
  merge
    work.name_file2  (in=in_sas)
    work.orcid_xlsx2 (in=in_xlsx)
  ;
  by cpfp_id;

  if in_sas and in_xlsx;
run;

proc sort data=work.people_lookup nodupkey;
  by cpfp_id;
run;

/*--------------------------------------------------------------*/
/* STEP 5: BUILD NO-ORCID REPORT                                */
/* include all available name-related variables                 */
/*--------------------------------------------------------------*/
data &no_orcid;
  merge
    work.name_file2  (in=in_sas)
    work.orcid_xlsx2 (in=in_xlsx)
  ;
  by cpfp_id;

  if in_sas and not in_xlsx;
run;

/*--------------------------------------------------------------*/
/* STEP 6: ADD NORMALIZED NAME VARIABLES                        */
/*--------------------------------------------------------------*/
data work.people_lookup_norm;
  set work.people_lookup;

  length
    sas_first_name_norm   $200
    sas_last_name_norm    $200
    excel_first_name_norm $200
    excel_last_name_norm  $200
    sas_full_name_norm    $500
    excel_full_name_norm  $500
    first_name_mismatch_flag 8
    last_name_mismatch_flag 8
    full_name_mismatch_flag 8
  ;

  /* SAS source */
  sas_first_name_norm = upcase(strip(first_name));
  sas_last_name_norm  = upcase(strip(last_name));

  /* Excel source */
  excel_first_name_norm = upcase(strip(excel_first_name));
  excel_last_name_norm  = upcase(strip(excel_last_name));

  /* normalize common diacritics */
  sas_first_name_norm = translate(sas_first_name_norm,
    'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOOooooooUUUUuuuuNnCcYy',
    '¡¿¬ƒ√≈·‡‚‰„Â…» ÀÈËÍÎÕÃŒœÌÏÓÔ”“‘÷’ÿÛÚÙˆı¯⁄Ÿ€‹˙˘˚¸—Ò«Á›˝');
  sas_last_name_norm = translate(sas_last_name_norm,
    'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOOooooooUUUUuuuuNnCcYy',
    '¡¿¬ƒ√≈·‡‚‰„Â…» ÀÈËÍÎÕÃŒœÌÏÓÔ”“‘÷’ÿÛÚÙˆı¯⁄Ÿ€‹˙˘˚¸—Ò«Á›˝');
  excel_first_name_norm = translate(excel_first_name_norm,
    'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOOooooooUUUUuuuuNnCcYy',
    '¡¿¬ƒ√≈·‡‚‰„Â…» ÀÈËÍÎÕÃŒœÌÏÓÔ”“‘÷’ÿÛÚÙˆı¯⁄Ÿ€‹˙˘˚¸—Ò«Á›˝');
  excel_last_name_norm = translate(excel_last_name_norm,
    'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOOooooooUUUUuuuuNnCcYy',
    '¡¿¬ƒ√≈·‡‚‰„Â…» ÀÈËÍÎÕÃŒœÌÏÓÔ”“‘÷’ÿÛÚÙˆı¯⁄Ÿ€‹˙˘˚¸—Ò«Á›˝');

  /* remove punctuation / spaces for matching */
  sas_first_name_norm   = compress(sas_first_name_norm,   " .,'`-()/");
  sas_last_name_norm    = compress(sas_last_name_norm,    " .,'`-()/");
  excel_first_name_norm = compress(excel_first_name_norm, " .,'`-()/");
  excel_last_name_norm  = compress(excel_last_name_norm,  " .,'`-()/");

  /* strip common suffixes from last-name-side comparisons if they leaked in */
  if sas_last_name_norm in ('JR','SR','II','III','IV') then sas_last_name_norm = '';
  if excel_last_name_norm in ('JR','SR','II','III','IV') then excel_last_name_norm = '';

  sas_full_name_norm   = catx('', sas_first_name_norm, sas_last_name_norm);
  excel_full_name_norm = catx('', excel_first_name_norm, excel_last_name_norm);

  first_name_mismatch_flag = 0;
  last_name_mismatch_flag  = 0;
  full_name_mismatch_flag  = 0;

  if not missing(excel_first_name_norm) and not missing(sas_first_name_norm)
     and excel_first_name_norm ne sas_first_name_norm then first_name_mismatch_flag = 1;

  if not missing(excel_last_name_norm) and not missing(sas_last_name_norm)
     and excel_last_name_norm ne sas_last_name_norm then last_name_mismatch_flag = 1;

  if not missing(excel_full_name_norm) and not missing(sas_full_name_norm)
     and excel_full_name_norm ne sas_full_name_norm then full_name_mismatch_flag = 1;
run;

/*--------------------------------------------------------------*/
/* STEP 7: BUILD MISMATCH REPORT                                */
/*--------------------------------------------------------------*/
data &mismatch_out;
  set work.people_lookup_norm;
  if first_name_mismatch_flag=1
     or last_name_mismatch_flag=1
     or full_name_mismatch_flag=1;
run;

/*--------------------------------------------------------------*/
/* STEP 8: PERSON-LEVEL FULL REPORT                             */
/*--------------------------------------------------------------*/
proc sort data=&person_in;
  by cpfp_id;
run;

proc sort data=work.people_lookup_norm;
  by cpfp_id;
run;

data &person_full;
  merge
    work.people_lookup_norm (in=a)
    &person_in              (in=b)
  ;
  by cpfp_id;

  if a or b;
run;

/*--------------------------------------------------------------*/
/* STEP 9: PUBLICATION-LEVEL FULL REPORT                        */
/*--------------------------------------------------------------*/
proc sort data=&pubs_in;
  by cpfp_id;
run;

data &pub_full;
  merge
    &pubs_in                (in=a)
    work.people_lookup_norm (in=b)
  ;
  by cpfp_id;

  if a;
run;

/*--------------------------------------------------------------*/
/* STEP 10: EXPORTS                                             */
/*--------------------------------------------------------------*/
proc export
  data=&person_full
  outfile="&person_xlsx"
  dbms=xlsx
  replace;
  sheet="person_full";
run;

proc export
  data=&pub_full
  outfile="&pub_xlsx"
  dbms=xlsx
  replace;
  sheet="publication_full";
run;

proc export
  data=&no_orcid
  outfile="&no_orcid_xlsx"
  dbms=xlsx
  replace;
  sheet="no_orcid";
run;

proc export
  data=&mismatch_out
  outfile="&mismatch_xlsx"
  dbms=xlsx
  replace;
  sheet="mismatches";
run;

/*--------------------------------------------------------------*/
/* STEP 11: REVIEW                                              */
/*--------------------------------------------------------------*/
title "Publication dataset sample";
proc print data=&pubs_in(obs=20);
run;

title "Person summary sample";
proc print data=&person_in(obs=20);
run;

title "Person full report";
proc print data=&person_full(obs=20);
run;

title "Publication full report";
proc print data=&pub_full(obs=20);
run;

title "People with no ORCID";
proc print data=&no_orcid(obs=20);
run;

title "Name mismatch report";
proc print data=&mismatch_out(obs=20);
run;

title;