/*========================================================*
 | ORCID DAILY PULL PROGRAM (UPDATED)
 | - Adds datetime stamp to output
 | - Removes ORCID2 بالكامل
 *========================================================*/

/*-----------------------------*
 | OPTIONS
 *-----------------------------*/
options mprint mlogic symbolgen;

/*-----------------------------*
 | DATE + TIME STAMP
 *-----------------------------*/
%let run_dt = %sysfunc(datetime(), b8601dt.);
%let run_dt = %sysfunc(compress(&run_dt.,:-));

/*-----------------------------*
 | FILE PATHS
 *-----------------------------*/
%let in_xlsx  = /prj/dcp/restricted/cpfp_eval/progs/orcidtesting/source/Alumni List with Orcids.xlsx;
%let out_xlsx = /prj/dcp/restricted/cpfp_eval/progs/orcidtesting/orcid_alumni_review_&run_dt..xlsx;

/*-----------------------------*
 | STEP 1: READ EXCEL
 *-----------------------------*/
proc import datafile="&in_xlsx."
    out=alumni_raw
    dbms=xlsx
    replace;
    sheet="Sheet1";
    getnames=yes;
run;

/*-----------------------------*
 | STEP 2: CLEAN ORCID
 *-----------------------------*/
data alumni_clean;
    set alumni_raw;

    length orcid_clean $19;

    /* Normalize ORCID */
    orcid_clean = compress(upcase(orcid), ' ');
    orcid_clean = tranwrd(orcid_clean, 'HTTP://ORCID.ORG/', '');
    orcid_clean = tranwrd(orcid_clean, 'HTTPS://ORCID.ORG/', '');

run;

/*-----------------------------*
 | STEP 3: KEEP VALID ORCID ROWS
 *-----------------------------*/
data alumni_orcid;
    set alumni_clean;

    length orcid $19 source $10;

    if not missing(orcid_clean) then do;
        orcid = orcid_clean;
        source = "ORCID";
        output;
    end;

run;

/*-----------------------------*
 | STEP 4: ORCID API CALL MACRO
 *-----------------------------*/
%macro http_get(url=, outref=resp);

    filename &outref temp;

    proc http
        url="&url."
        method="GET"
        out=&outref.;
        headers
            "Accept"="application/json";
    run;

%mend;

/*-----------------------------*
 | STEP 5: PULL PUBLICATIONS
 *-----------------------------*/
%macro get_orcid_pubs(in_ds=, out_ds=);

    data &out_ds.;
        length orcid $19 title $500 journal $200 pub_year 8;
        stop;
    run;

    data _null_;
        set &in_ds.;
        call symputx(cats('orcid',_n_), orcid);
        call symputx('nobs', _n_);
    run;

    %do i=1 %to &nobs.;

        %let this_orcid = &&orcid&i.;

        %let url = https://pub.orcid.org/v3.0/&this_orcid./works;

        %http_get(url=&url., outref=wkresp);

        libname wk json fileref=wkresp;

        data tmp;
            set wk.group;

            length orcid $19;
            orcid = "&this_orcid.";

        run;

        proc append base=&out_ds. data=tmp force;
        run;

        libname wk clear;

    %end;

%mend;

/*-----------------------------*
 | STEP 6: RUN API
 *-----------------------------*/
%get_orcid_pubs(
    in_ds=alumni_orcid,
    out_ds=all_orcid_publications
);

/*-----------------------------*
 | STEP 7: PERSON SUMMARY
 *-----------------------------*/
proc sql;
    create table person_summary as
    select 
        a.orcid,
        count(*) as publication_count
    from all_orcid_publications a
    group by a.orcid;
quit;

/*-----------------------------*
 | STEP 8: EXPORT TO EXCEL
 *-----------------------------*/
ods excel file="&out_xlsx." options(sheet_interval="none");

ods excel options(sheet_name="person_simplified");

proc print data=person_summary noobs;
run;

ods excel options(sheet_name="publications");

proc print data=all_orcid_publications noobs;
run;

ods excel close;

/*-----------------------------*
 | DONE
 *-----------------------------*/
%put NOTE: Output written to &out_xlsx.;