/*====================================================================*
 | PROGRAM: ORCID PROFILE TO SAS DATASETS                              |
 | PURPOSE: Loop over a list of ORCID IDs, download public profile     |
 |          payloads from ORCID, and store them in SAS datasets.       |
 |                                                                      |
 | NOTE: ORCID /record endpoint already contains all publicly visible   |
 |       profile data. Extra endpoints are optional and can help with   |
 |       downstream processing without reparsing the full record JSON.  |
 *====================================================================*/

options spool source2 mprint mlogic symbolgen;

/*-----------------------------*
 | USER SETTINGS               |
 *-----------------------------*/
/* Input list: ORCID IDs come from this Excel file/sheet (column: ORCID). */
%let excel_file  = /prj/dcp/restricted/cpfp_eval/progs/orcidtesting/source/cpfp_merged_orcid.xlsx;
%let excel_sheet = cpfp_merged;
%let input_ds    = work.orcid_input;

/* Permanent output library for .sas7bdat outputs. */
libname outlib "/prj/dcp/restricted/cpfp_eval/progs/orcidtesting/fullpull";

/* Optional API token. Leave blank for public endpoints/rate limits. */
%let orcid_token = ;

/* API version + base URL. */
%let api_version = v3.0;
%let api_base    = https://pub.orcid.org/&api_version;

/*-----------------------------*
 | STEP 1: IMPORT EXCEL INPUT  |
 *-----------------------------*/
proc import
  datafile="&excel_file"
  out=&input_ds
  dbms=xlsx
  replace;
  sheet="&excel_sheet";
  getnames=yes;
run;

/*-----------------------------*
 | STEP 2: CLEAN + VALIDATE    |
 *-----------------------------*/
data work.orcid_ids;
  length orcid $19;
  set &input_ds;

  /* Keep only valid ORCID characters and expected length. */
  orcid = strip(compress(vvalue(orcid), '0123456789Xx-', 'k'));

  if prxmatch('/^\d{4}-\d{4}-\d{4}-\d{3}[0-9Xx]$/', orcid);
  keep orcid;
run;

proc sort data=work.orcid_ids nodupkey;
  by orcid;
run;

/*-----------------------------*
 | INIT OUTPUT DATASETS        |
 *-----------------------------*/
%macro init_raw_ds(ds=);
  data &ds;
    length
      orcid $19
      endpoint $32
      request_url $500
      status_code 8
      status_phrase $120
      pulled_at_utc 8
      line_no 8
      json_line $32767
    ;
    format pulled_at_utc e8601dt19.;
    stop;
  run;
%mend;

%init_raw_ds(ds=work.record_raw);
%init_raw_ds(ds=work.works_raw);
%init_raw_ds(ds=work.employments_raw);
%init_raw_ds(ds=work.educations_raw);
%init_raw_ds(ds=work.fundings_raw);
%init_raw_ds(ds=work.peer_reviews_raw);

/* Status dataset to monitor pull success/failure per endpoint. */
data work.pull_status;
  length
    orcid $19
    endpoint $32
    request_url $500
    status_code 8
    status_phrase $120
    pulled_at_utc 8
  ;
  format pulled_at_utc e8601dt19.;
  stop;
run;

/*-----------------------------*
 | GENERIC ENDPOINT PULL       |
 *-----------------------------*/
%macro pull_endpoint(orcid=, endpoint=record, outds=work.record_raw);

  %local request_url;
  %let request_url = &api_base/%superq(orcid)/&endpoint;

  filename oresp temp;

  proc http
    method="GET"
    url="&request_url"
    out=oresp
    %if %length(&orcid_token) %then %do;
      oauth_bearer="&orcid_token"
    %end;
  ;
    headers
      "Accept"="application/json"
      "Content-Type"="application/json"
    ;
  run;

  %let _code   = &SYS_PROCHTTP_STATUS_CODE;
  %let _phrase = %superq(SYS_PROCHTTP_STATUS_PHRASE);

  data work._status_one;
    length
      orcid $19
      endpoint $32
      request_url $500
      status_code 8
      status_phrase $120
      pulled_at_utc 8
    ;
    format pulled_at_utc e8601dt19.;

    orcid         = "&orcid";
    endpoint      = "&endpoint";
    request_url   = "&request_url";
    status_code   = input("&_code", best.);
    status_phrase = "&_phrase";
    pulled_at_utc = datetime();
  run;

  proc append base=work.pull_status data=work._status_one force;
  run;

  data work._raw_one;
    length
      orcid $19
      endpoint $32
      request_url $500
      status_code 8
      status_phrase $120
      pulled_at_utc 8
      line_no 8
      json_line $32767
    ;
    format pulled_at_utc e8601dt19.;

    infile oresp lrecl=32767 truncover end=eof;
    input json_line $char32767.;

    orcid         = "&orcid";
    endpoint      = "&endpoint";
    request_url   = "&request_url";
    status_code   = input("&_code", best.);
    status_phrase = "&_phrase";
    pulled_at_utc = datetime();
    line_no + 1;

    output;

    if eof and line_no=0 then do;
      json_line = '';
      output;
    end;
  run;

  proc append base=&outds data=work._raw_one force;
  run;

  proc datasets lib=work nolist;
    delete _status_one _raw_one;
  quit;

  filename oresp clear;

%mend;

/*-----------------------------*
 | LOOP THROUGH ALL ORCIDS     |
 *-----------------------------*/
%macro run_all_orcids;

  proc sql noprint;
    select count(*) into :n_orcid trimmed
    from work.orcid_ids;
  quit;

  %if &n_orcid = 0 %then %do;
    %put ERRANT: No valid ORCID IDs found in &input_ds..;
    %return;
  %end;

  %do i = 1 %to &n_orcid;

    data _null_;
      set work.orcid_ids(firstobs=&i obs=&i);
      call symputx('this_orcid', strip(orcid), 'l');
    run;

    /* Full public profile payload (recommended primary source). */
    %pull_endpoint(orcid=&this_orcid, endpoint=record,      outds=work.record_raw);

    /* Optional section-level pulls for easier downstream parsing. */
    %pull_endpoint(orcid=&this_orcid, endpoint=works,       outds=work.works_raw);
    %pull_endpoint(orcid=&this_orcid, endpoint=employments, outds=work.employments_raw);
    %pull_endpoint(orcid=&this_orcid, endpoint=educations,  outds=work.educations_raw);
    %pull_endpoint(orcid=&this_orcid, endpoint=fundings,    outds=work.fundings_raw);
    %pull_endpoint(orcid=&this_orcid, endpoint=peer-reviews,outds=work.peer_reviews_raw);

  %end;

%mend;

%run_all_orcids;

/*-----------------------------*
 | SAVE SAS DATASETS           |
 *-----------------------------*/
data outlib.orcid_pull_status(compress=binary);
  set work.pull_status;
run;

data outlib.orcid_record_raw(compress=binary);
  set work.record_raw;
run;

data outlib.orcid_works_raw(compress=binary);
  set work.works_raw;
run;

data outlib.orcid_employments_raw(compress=binary);
  set work.employments_raw;
run;

data outlib.orcid_educations_raw(compress=binary);
  set work.educations_raw;
run;

data outlib.orcid_fundings_raw(compress=binary);
  set work.fundings_raw;
run;

data outlib.orcid_peer_reviews_raw(compress=binary);
  set work.peer_reviews_raw;
run;

/*-----------------------------*
 | QUICK QC                    |
 *-----------------------------*/
title "ORCID pull status";
proc freq data=outlib.orcid_pull_status;
  tables endpoint*status_code / missing;
run;

title "Sample pulled JSON rows (record endpoint)";
proc print data=outlib.orcid_record_raw(obs=25);
run;
title;
