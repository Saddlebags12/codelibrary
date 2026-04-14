/* ============================================================
   FULL PROGRAM: ORCID candidate pull + institution scoring
   Key fields: cpfp_id, first_name, last_name
   Institution: National Cancer Institute
   ============================================================ */

options obs=max nosyntaxcheck mprint mlogic symbolgen spool;

/* ---------------------------
   1) Import source spreadsheet
   --------------------------- */
proc import datafile="/prj/dcp/restricted/cpfp_eval/progs/orcidtesting/orcid_reporting/no_orcid_report.xlsx"
    out=work.names_raw
    dbms=xlsx
    replace;
    sheet="no_orcid";
    getnames=yes;
run;

/* ---------------------------
   2) Standardize input fields
   --------------------------- */
data work.names_std;
    set work.names_raw;
    length cpfp_id $50 first_name last_name $100;
    cpfp_id    = strip(cpfp_id);
    first_name = compbl(strip(first_name));
    last_name  = compbl(strip(last_name));
    if missing(cpfp_id) or missing(first_name) or missing(last_name) then delete;
run;

/* ----------------------------------------------------
   3) Build ORCID broad-search URLs (name-only query)
   ---------------------------------------------------- */
data work.names_q;
    set work.names_std;
    length q_first q_last $200 query_url $1000;
    q_first = tranwrd(first_name, ' ', '%20');
    q_last  = tranwrd(last_name,  ' ', '%20');

    query_url = cats(
      "https://pub.orcid.org/v3.0/search/?q=",
      "given-names:", q_first, "%20AND%20family-name:", q_last
    );
run;

/* -----------------------------------------------------------------
   4) Pull ORCID candidates per person from broad-search results
   ----------------------------------------------------------------- */
%macro get_orcid_candidates();

  data work.orcid_hits_all;
    length cpfp_id $50 first_name last_name $100 orcid_identifier_path $25;
    stop;
  run;

  proc sql noprint;
    select count(*) into :nobs trimmed
    from work.names_q;
  quit;

  %do i = 1 %to &nobs;

    data _null_;
      set work.names_q(firstobs=&i obs=&i);
      call symputx('v_cpfp_id', cpfp_id, 'l');
      call symputx('v_first',   first_name, 'l');
      call symputx('v_last',    last_name, 'l');
      call symputx('v_url',     query_url, 'l');
    run;

    filename resp temp;

    proc http
      method="GET"
      url="%superq(v_url)"
      out=resp;
      headers "Accept"="application/json";
    run;

    data work.orcid_hits_one;
      length cpfp_id $50 first_name last_name $100 line $32767 orcid_identifier_path $25;
      retain re;
      if _n_=1 then re = prxparse('/\b\d{4}-\d{4}-\d{4}-\d{3}[0-9X]\b/');

      cpfp_id    = symget('v_cpfp_id');
      first_name = symget('v_first');
      last_name  = symget('v_last');

      infile resp lrecl=32767 truncover;
      input line $char32767.;

      start = 1; stop = length(line);
      call prxnext(re, start, stop, line, pos, len);
      do while (pos > 0);
        orcid_identifier_path = substr(line, pos, len);
        output;
        call prxnext(re, start, stop, line, pos, len);
      end;

      keep cpfp_id first_name last_name orcid_identifier_path;
    run;

    proc sort data=work.orcid_hits_one nodupkey;
      by cpfp_id first_name last_name orcid_identifier_path;
    run;

    proc sql noprint;
      select count(*) into :hit_n trimmed
      from work.orcid_hits_one;
    quit;

    %if &hit_n = 0 %then %do;
      data work.orcid_hits_one;
        length cpfp_id $50 first_name last_name $100 orcid_identifier_path $25;
        cpfp_id    = symget('v_cpfp_id');
        first_name = symget('v_first');
        last_name  = symget('v_last');
        orcid_identifier_path = "";
      run;
    %end;

    proc append base=work.orcid_hits_all data=work.orcid_hits_one force;
    run;

    filename resp clear;

  %end;  /* i */

%mend get_orcid_candidates;

%get_orcid_candidates();

/* --------------------------------------
   5) Build candidate table and hit counts
   -------------------------------------- */
proc sql;
  create table work.candidates as
  select distinct
         a.cpfp_id,
         a.first_name,
         a.last_name,
         tranwrd(strip(a.first_name),' ','%20') as q_first length=200,
         tranwrd(strip(a.last_name),' ','%20')  as q_last  length=200,
         a.orcid_identifier_path as orcid_id length=25
  from work.orcid_hits_all a
  where not missing(a.orcid_identifier_path);

  create table work.candidate_counts as
  select cpfp_id,
         count(distinct orcid_id) as candidate_n
  from work.candidates
  group by cpfp_id;
quit;

/* -----------------------------------------------------------------
   6) Score each candidate with institution-constrained ORCID query
   ----------------------------------------------------------------- */
%macro score_candidates(inst_name=National%20Cancer%20Institute);

  data work.scored_candidates;
    length cpfp_id $50 first_name last_name $100 orcid_id $25 institution_match 8;
    stop;
  run;

  proc sql noprint;
    select count(*) into :nobs trimmed
    from work.candidates;
  quit;

  %do i = 1 %to &nobs;

    data _null_;
      set work.candidates(firstobs=&i obs=&i);
      call symputx('v_cpfp_id', cpfp_id, 'l');
      call symputx('v_first',   first_name, 'l');
      call symputx('v_last',    last_name, 'l');
      call symputx('v_qfirst',  q_first, 'l');
      call symputx('v_qlast',   q_last, 'l');
      call symputx('v_orcid',   orcid_id, 'l');
    run;

    filename resp temp;

    proc http
      method="GET"
      url="https://pub.orcid.org/v3.0/search/?q=given-names:%superq(v_qfirst)%20AND%20family-name:%superq(v_qlast)%20AND%20affiliation-org-name:%22&inst_name%22"
      out=resp;
      headers "Accept"="application/json";
    run;

    data work.one_score;
      length cpfp_id $50 first_name last_name $100 orcid_id $25 line $32767;
      retain found 0;
      cpfp_id    = symget('v_cpfp_id');
      first_name = symget('v_first');
      last_name  = symget('v_last');
      orcid_id   = symget('v_orcid');

      infile resp lrecl=32767 truncover end=eof;
      input line $char32767.;

      if index(line, strip(orcid_id)) > 0 then found=1;

      if eof then do;
        institution_match = found;
        output;
      end;

      keep cpfp_id first_name last_name orcid_id institution_match;
    run;

    proc append base=work.scored_candidates data=work.one_score force;
    run;

    filename resp clear;

  %end;  /* i */

%mend score_candidates;

%score_candidates();

/* -----------------------------------------
   7) Compute score + confidence + best match
   ----------------------------------------- */
proc sql;
  create table work.scored_candidates2 as
  select a.*,
         b.candidate_n,
         (60
          + case when a.institution_match=1 then 30 else 0 end
          + case when b.candidate_n=1 then 10 else 0 end
         ) as score
  from work.scored_candidates a
  left join work.candidate_counts b
    on a.cpfp_id=b.cpfp_id;
quit;

data work.scored_candidates3;
  set work.scored_candidates2;
  length confidence $12;
  if score >= 90 then confidence='HIGH';
  else if score >= 70 then confidence='MEDIUM';
  else confidence='LOW';
run;

proc sort data=work.scored_candidates3;
  by cpfp_id descending score descending institution_match orcid_id;
run;

data work.best_orcid_per_cpfp;
  set work.scored_candidates3;
  by cpfp_id;
  if first.cpfp_id then do;
    selected_orcid_id   = orcid_id;
    selected_score      = score;
    selected_confidence = confidence;
    output;
  end;
  keep cpfp_id first_name last_name selected_orcid_id selected_score
       selected_confidence candidate_n institution_match;
run;

/* --------------------------------------------------------
   8) Final output including people with no selected ORCID
   -------------------------------------------------------- */
proc sql;
  create table work.final_orcid_match as
  select n.cpfp_id,
         n.first_name,
         n.last_name,
         b.selected_orcid_id,
         b.selected_score,
         b.selected_confidence,
         coalesce(b.candidate_n,0) as candidate_n,
         case when b.selected_orcid_id is not null then 1 else 0 end as has_selected_orcid
  from work.names_std n
  left join work.best_orcid_per_cpfp b
    on n.cpfp_id=b.cpfp_id;
quit;

/* ---------------------
   9) Export output files
   --------------------- */
proc export data=work.scored_candidates3
  outfile="/prj/dcp/restricted/cpfp_eval/progs/orcidtesting/orcid_candidate_details.xlsx"
  dbms=xlsx
  replace;
run;

proc export data=work.final_orcid_match
  outfile="/prj/dcp/restricted/cpfp_eval/progs/orcidtesting/final_orcid_match_scored.xlsx"
  dbms=xlsx
  replace;
run;