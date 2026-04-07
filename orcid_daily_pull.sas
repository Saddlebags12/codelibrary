/*=====================================================================*
  ORCID ALUMNI CLEANUP
  ----------------------------------------------------------------------
  PURPOSE
    1. Read the alumni Excel file containing ORCID and ORCID2
    2. Clean and standardize both IDs
    3. Reshape to one row per person per ORCID field
    4. Query ORCID once per distinct cleaned ID
    5. Classify each ID as:
         1 = ORCID does not exist
         2 = ORCID exists, no publications listed
         3 = ORCID exists, >=1 publication listed
    6. Pull ORCID name for valid records
    7. Compare alumni name vs ORCID name
    8. Export review workbook

  IMPORTANT
    - This version does NOT include ORCID search fallback
    - This version does NOT require client_id / client_secret
    - This version assumes the Excel file contains:
         cpfp_id
         name
         ORCID
         ORCID2
 *=====================================================================*/

options mprint mlogic symbolgen;

/*-----------------------------*
 | STEP 0. SETTINGS
 *-----------------------------*/

%let excel_file  = /prj/dcp/restricted/cpfp_eval/progs/orcidtesting/source/Alumni List with Orcids.xlsx;
%let excel_sheet = Sheet1;

%let out_xlsx = /prj/dcp/restricted/cpfp_eval/progs/orcidtesting/orcid_alumni_review.xlsx;

%let api_base  = https://pub.orcid.org/v3.0;

/*-----------------------------*
 | STEP 1. READ ALUMNI EXCEL FILE
 *-----------------------------*/

proc import datafile="&excel_file"
    out=alumni_raw
    dbms=xlsx
    replace;
    sheet="&excel_sheet";
    getnames=yes;
run;

data alumni;
    set alumni_raw;

    length alumni_name_clean $200 cpfp_id_char $50;

    alumni_name_clean = compbl(strip(name));
    cpfp_id_char      = strip(vvalue(cpfp_id));
run;

/*-----------------------------*
 | STEP 2. CLEAN ORCID / ORCID2
 *-----------------------------*/

data alumni_clean;
    set alumni;

    length orcid_clean  $19
           orcid2_clean $19;

    /* Clean ORCID */
    _tmp1 = upcase(coalescec(strip(ORCID),''));
    _tmp1 = compress(_tmp1, '- ');
    if length(_tmp1)=16 then
        orcid_clean = cats(substr(_tmp1,1,4),'-',
                           substr(_tmp1,5,4),'-',
                           substr(_tmp1,9,4),'-',
                           substr(_tmp1,13,4));
    else orcid_clean='';

    if not prxmatch('/^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$/', strip(orcid_clean)) then
        orcid_clean='';

    /* Clean ORCID2 */
    _tmp2 = upcase(coalescec(strip(ORCID2),''));
    _tmp2 = compress(_tmp2, '- ');
    if length(_tmp2)=16 then
        orcid2_clean = cats(substr(_tmp2,1,4),'-',
                            substr(_tmp2,5,4),'-',
                            substr(_tmp2,9,4),'-',
                            substr(_tmp2,13,4));
    else orcid2_clean='';

    if not prxmatch('/^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$/', strip(orcid2_clean)) then
        orcid2_clean='';

    has_two_id_fields    = (not missing(orcid_clean) and not missing(orcid2_clean));
    has_two_distinct_ids = (not missing(orcid_clean) and not missing(orcid2_clean)
                            and orcid_clean ne orcid2_clean);
    duplicate_id_in_both = (not missing(orcid_clean) and not missing(orcid2_clean)
                            and orcid_clean = orcid2_clean);
    no_usable_orcid      = (missing(orcid_clean) and missing(orcid2_clean));

    drop _tmp1 _tmp2;
run;

/*-----------------------------*
 | STEP 3. RESHAPE TO LONG FORM
 *-----------------------------*/

data alumni_orcid_long alumni_no_orcid;
    set alumni_clean;

    length id_source $6
           submitted_orcid $50
           orcid_id $19;

    if no_usable_orcid then output alumni_no_orcid;

    if not missing(orcid_clean) then do;
        id_source       = 'ORCID';
        submitted_orcid = strip(ORCID);
        orcid_id        = orcid_clean;
        output alumni_orcid_long;
    end;

    if not missing(orcid2_clean) then do;
        id_source       = 'ORCID2';
        submitted_orcid = strip(ORCID2);
        orcid_id        = orcid2_clean;
        output alumni_orcid_long;
    end;
run;

/*-----------------------------*
 | STEP 4. DISTINCT IDs TO QUERY
 *-----------------------------*/

proc sort data=alumni_orcid_long(keep=orcid_id)
          out=distinct_orcid_ids nodupkey;
    by orcid_id;
run;

/*-----------------------------*
 | STEP 5. EMPTY OUTPUT TABLE
 *-----------------------------*/

data all_orcid_results;
    length orcid_id $19
           orcid_given_name $200
           orcid_family_name $200
           orcid_credit_name $200
           orcid_display_name $300
           record_http_status 8
           works_http_status 8
           pub_count 8
           orcid_status 8;
    stop;
run;

/*-----------------------------*
 | STEP 6. MACRO: QUERY ONE ORCID
 *-----------------------------*/

%macro check_orcid(orcid);

filename recresp temp;
filename wresp   temp;

%local record_status works_status orcid_status pub_count given family credit;

%let record_status=.;
%let works_status=.;
%let orcid_status=1;
%let pub_count=.;
%let given=;
%let family=;
%let credit=;

/* Read record */
proc http
    url="&api_base/&orcid/record"
    method="GET"
    out=recresp;
    headers
        "Accept"="application/json";
run;

%let record_status=&SYS_PROCHTTP_STATUS_CODE;

%if &record_status = 200 %then %do;

    libname rjson json fileref=recresp;

    %if %sysfunc(exist(rjson.name)) %then %do;
        data _null_;
            set rjson.name(obs=1);
            length _given _family _credit $200;
            _given = '';
            _family = '';
            _credit = '';

            array cvars {*} _character_;
            do i=1 to dim(cvars);
                if upcase(vname(cvars{i}))='GIVEN_NAMES_VALUE' then _given = strip(cvars{i});
                else if upcase(vname(cvars{i}))='GIVEN_NAMES' and missing(_given) then _given = strip(cvars{i});
                else if upcase(vname(cvars{i}))='FAMILY_NAME_VALUE' then _family = strip(cvars{i});
                else if upcase(vname(cvars{i}))='FAMILY_NAME' and missing(_family) then _family = strip(cvars{i});
                else if upcase(vname(cvars{i}))='CREDIT_NAME_VALUE' then _credit = strip(cvars{i});
                else if upcase(vname(cvars{i}))='CREDIT_NAME' and missing(_credit) then _credit = strip(cvars{i});
            end;

            call symputx('given',  _given,  'l');
            call symputx('family', _family, 'l');
            call symputx('credit', _credit, 'l');
        run;
    %end;

    libname rjson clear;

    /* Read works */
    proc http
        url="&api_base/&orcid/works"
        method="GET"
        out=wresp;
        headers
            "Accept"="application/json";
    run;

    %let works_status=&SYS_PROCHTTP_STATUS_CODE;

    %if &works_status = 200 %then %do;
        libname wjson json fileref=wresp;

        %if %sysfunc(exist(wjson.group)) %then %do;
            proc sql noprint;
                select count(*) into :pub_count trimmed
                from wjson.group;
            quit;
        %end;
        %else %let pub_count=0;

        libname wjson clear;

        %if %sysevalf(&pub_count > 0) %then %let orcid_status=3;
        %else %let orcid_status=2;
    %end;
    %else %do;
        %let orcid_status=2;
        %let pub_count=.;
    %end;
%end;
%else %do;
    %let orcid_status=1;
%end;

data one_orcid_result;
    length orcid_id $19
           orcid_given_name $200
           orcid_family_name $200
           orcid_credit_name $200
           orcid_display_name $300;

    orcid_id            = "&orcid";
    record_http_status  = input("&record_status", best12.);
    works_http_status   = input("&works_status", best12.);
    orcid_status        = input("&orcid_status", best12.);
    pub_count           = input("&pub_count", best12.);

    orcid_given_name    = symget('given');
    orcid_family_name   = symget('family');
    orcid_credit_name   = symget('credit');

    if not missing(orcid_credit_name) then
        orcid_display_name = strip(orcid_credit_name);
    else
        orcid_display_name = catx(' ', strip(orcid_given_name), strip(orcid_family_name));
run;

proc append base=all_orcid_results data=one_orcid_result force;
run;

%mend check_orcid;

/*-----------------------------*
 | STEP 7. BUILD MACRO VARS
 *-----------------------------*/

data _null_;
    set distinct_orcid_ids end=last;
    call symputx(cats('id',_n_), orcid_id, 'g');
    if last then call symputx('n_ids', _n_, 'g');
run;

/*-----------------------------*
 | STEP 8. QUERY ALL DISTINCT IDs
 *-----------------------------*/

%macro run_all;
    %if %symexist(n_ids) %then %do;
        %do i=1 %to &n_ids;
            %put NOTE: QUERYING ORCID &&id&i (&i of &n_ids);
            %check_orcid(&&id&i);
        %end;
    %end;
%mend run_all;

%run_all;

/*-----------------------------*
 | STEP 9. MERGE RESULTS BACK
 *-----------------------------*/

proc sql;
    create table alumni_orcid_review as
    select a.*,
           b.record_http_status,
           b.works_http_status,
           b.orcid_status,
           b.pub_count,
           b.orcid_given_name,
           b.orcid_family_name,
           b.orcid_credit_name,
           b.orcid_display_name
    from alumni_orcid_long as a
    left join all_orcid_results as b
      on a.orcid_id = b.orcid_id
    ;
quit;

/*-----------------------------*
 | STEP 10. NAME COMPARE / FLAGS
 *-----------------------------*/

data alumni_orcid_review2;
    set alumni_orcid_review;

    length review_flag $100
           name_compare_flag $60
           alumni_name_std $300
           orcid_name_std $300;

    alumni_name_std = upcase(compress(alumni_name_clean, , 'kas'));
    orcid_name_std  = upcase(compress(orcid_display_name, , 'kas'));

    if has_two_distinct_ids then
        review_flag = 'Two different ORCIDs listed for this person';
    else if duplicate_id_in_both then
        review_flag = 'Same ORCID entered in both fields';
    else if orcid_status = 1 then
        review_flag = 'ORCID does not exist';
    else if orcid_status in (2,3) and missing(orcid_display_name) then
        review_flag = 'ORCID exists, name not parsed';
    else review_flag = '';

    if orcid_status in (2,3) then do;
        if alumni_name_std = orcid_name_std then
            name_compare_flag = 'Exact standardized match';
        else if not missing(orcid_name_std) then
            name_compare_flag = 'Review name mismatch';
        else
            name_compare_flag = '';
    end;
    else name_compare_flag = '';
run;

/*-----------------------------*
 | STEP 11. REVIEW TABLES
 *-----------------------------*/

proc sql;
    create table people_with_two_ids as
    select *
    from alumni_orcid_review2
    where has_two_id_fields = 1
    order by alumni_name_clean, id_source
    ;

    create table name_mismatch_review as
    select *
    from alumni_orcid_review2
    where orcid_status in (2,3)
      and name_compare_flag ne 'Exact standardized match'
    order by alumni_name_clean, id_source
    ;

    create table same_orcid_multiple_people as
    select orcid_id,
           count(*) as n_rows,
           count(distinct alumni_name_clean) as n_names
    from alumni_orcid_long
    group by orcid_id
    having calculated n_names > 1
    order by orcid_id
    ;

    create table distinct_orcid_results_sorted as
    select *
    from all_orcid_results
    order by orcid_status, orcid_id
    ;
quit;

data people_with_no_usable_orcid;
    set alumni_no_orcid;
run;

/*-----------------------------*
 | STEP 12. SUMMARY COUNTS
 *-----------------------------*/

proc sql;
    create table summary_counts as
    select 'Distinct ORCIDs queried' as metric length=60, count(*) as value from distinct_orcid_ids
    union all
    select 'ORCID does not exist', count(*) from all_orcid_results where orcid_status=1
    union all
    select 'ORCID exists, no publications', count(*) from all_orcid_results where orcid_status=2
    union all
    select 'ORCID exists, >=1 publication', count(*) from all_orcid_results where orcid_status=3
    union all
    select 'People with two populated ID fields', count(*) from alumni_clean where has_two_id_fields=1
    union all
    select 'People with two distinct IDs', count(*) from alumni_clean where has_two_distinct_ids=1
    union all
    select 'People with no usable ORCID', count(*) from alumni_no_orcid
    ;
quit;

/*-----------------------------*
 | STEP 13. EXPORT TO EXCEL
 *-----------------------------*/

ods excel file="&out_xlsx"
    options(sheet_interval='none'
            embedded_titles='yes'
            frozen_headers='yes'
            autofilter='all');

ods excel options(sheet_name="Main_Review");
title "Main ORCID Review";
proc print data=alumni_orcid_review2 noobs;
run;

ods excel options(sheet_name="Two_IDs_Listed");
title "People with ORCID and ORCID2";
proc print data=people_with_two_ids noobs;
run;

ods excel options(sheet_name="Name_Mismatch");
title "Name Mismatch Review";
proc print data=name_mismatch_review noobs;
run;

ods excel options(sheet_name="Same_ID_MultiPeople");
title "Same ORCID Used for Multiple People";
proc print data=same_orcid_multiple_people noobs;
run;

ods excel options(sheet_name="Distinct_ORCID_Results");
title "Distinct ORCID Query Results";
proc print data=distinct_orcid_results_sorted noobs;
run;

ods excel options(sheet_name="No_Usable_ORCID");
title "People with No Usable ORCID";
proc print data=people_with_no_usable_orcid noobs;
run;

ods excel options(sheet_name="Summary_Counts");
title "Summary Counts";
proc print data=summary_counts noobs;
run;

ods excel close;
title;

/*-----------------------------*
 | STEP 14. OPTIONAL QUICK CHECKS
 *-----------------------------*/

proc freq data=all_orcid_results;
    tables orcid_status / missing;
    title "ORCID Status Distribution";
run;

proc freq data=alumni_clean;
    tables has_two_id_fields has_two_distinct_ids duplicate_id_in_both no_usable_orcid / missing;
    title "Original Alumni ID Flags";
run;

title;