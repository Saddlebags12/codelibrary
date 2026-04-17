ods path(prepend) work.template(update);  **this line prevents problems when you run multiple programs simultaneously;




filename macs '/prj/plcoims/database/lab_specimen/blood/progs/programs/masterfiles/blood_include.sas';
%inc macs;
&vialcreatefiles	

proc contents data=all_vials;

	
		
****************************************************************;
****************************************************************;
****************************************************************;
********************* Bad Serum Aliquots ***********************;
************** From Ovarian Study 2009-00504 *******************;
****************************************************************;
****************************************************************;
****************************************************************;
		data ovarbad1;
			set ovarbad (keep= bsi_id rename=(bsi_id = parent));
		run;
		
		proc sort data=all_vials tagsort; by parent;
		proc sort data=ovarbad1 tagsort; by parent;
			
		data all_vials;
			merge all_vials (in=invial)
						ovarbad1 (in=inovar);
			by parent;
			if invial;
			
			if inovar then in_ovar = 1;
		run;
		
		
****************************************************************;
****************************************************************;
****************************************************************;
********************* Discordant Vials *************************;
****************************************************************;
****************************************************************;
****************************************************************;
****************************************************************;
	proc sort data=discordant tagsort; by sampleid;
	proc sort data=all_vials tagsort; by sampleid;
	
	data dna_discordant (keep=sampleid);
		set all_vials;
		by sampleid;
		
		retain has_discord;
		if first.sampleid then has_discord = 0;
		if dna_usability = 11 then has_discord = 1;
		if sampleid = 'PL83525' then has_discord = 1; ***** mike furr was informed by DESL that this sample id is gender discordant needs replaced;
		if last.sampleid and has_discord = 1 then output;
	run;		
		
	data discordant;
		set discordant; 
		by sampleid;
		if last.sampleid;
	run;
	
	data all_discordant;
		merge discordant
					dna_discordant;
		by sampleid;
	run;
	
	data all_vials;
		merge all_vials (in=inspec)
					all_discordant (in=indis);
		by sampleid;
		if inspec;
		in_discord = indis;
	run;		
	

	
****************************************************************;
****************************************************************;
****************************************************************;
***************** Whole Blood Child Vials **********************;
****************** Match BCF Sample IDs ************************;
****************************************************************;
****************************************************************;
****************************************************************;
****************************************************************;	
	
	proc sort data=all_vials tagsort; by sampleid seq_num;
	data source_vials;
		set all_vials (keep= bsi_id seq_num sampleid vcode1-vcode15 vcomm1-vcomm15 mattype bcf_sampleid iid old_iid dt_draw dt_rcvd);
		by sampleid;
		if seq_num <= '0113' or (seq_num in ('0133','0134') and mattype = 'CC');
	run;
		
	data source_vials;
		set source_vials;
		by sampleid;		
		array a_vcode[1:15] vcode1-vcode15;	
		array a_vcomm[1:15] vcomm1-vcomm15;	
		
		if mattype = 'B4' then do i = 1 to 15;
			if a_vcode[i] = 'SAMPLEID' then bcf_sampleid = cat(substr(a_vcomm[i],1,2),' ',substr(a_vcomm[i],3,4));
		end;
		
		if last.sampleid then output;							
	run;
				
	data sampleid;
		merge all_vials (keep= bsi_id sampleid seq_num)
					source_vials (keep= sampleid bcf_sampleid iid old_iid dt_draw);
		by sampleid;
	run;
	
	

****************************************************************;
****************************************************************;
****************************************************************;
************************* Process Labship **********************;
****************************************************************;
****************************************************************;
****************************************************************;
****************************************************************;
		
/*	proc sort data=labshipp; by sample_id vialnum eems_id descending date;*/
	
	%macro replace_hyphens(variable);
		&variable. = trim(&variable.);
		length_var =length(&variable.);
		
		do k = 1 to length_var;
			if substr(&variable.,k,1) in ("-"," ",".") then do;
				if k ^= length_var then &variable. = substr(&variable.,1,k-1) || "_" || substr(&variable.,k+1,length_var - k);
				else if k = length_var then &variable. = substr(&variable.,1,k-1) || "_";
			end;
		end;
	%mend;
	
	data labship;
		set labship;
		if redacted ne 1 and not missing(bsi_id);
		
		if eems_id in ('gsa_buccal_reex','gsa_sister_vial') then eems_id = 'Global Screen Array';		
	run;
	
	
	
	proc sort data=labids tagsort; by eems_id;
	proc sort data=labstudy tagsort; by eems_id;
		
	data labstudy;
		merge labstudy (in=instudy keep=eems_id analyte_category)
					labids (in=inids);
		by eems_id;
		
		in_study = instudy;
		in_ids = inids;
		
		
		%replace_hyphens(ship_id);
	run;
	
	
	data labship;
		set labship;		
		
		length ship_id $ 50;
		ship_id = eems_id;
		%replace_hyphens(ship_id);
	run;
	
	proc sort data=labship tagsort; by ship_id;
	proc sort data=labstudy tagsort; by ship_id;
		
	data labship;
		merge labship (in=inship )
					labstudy (in=instudy keep=ship_id analyte_category);
		by ship_id;
		
		in_ship = inship;
		in_study = instudy;
	run;
	
		
	
	proc sort data=labship tagsort; by eems_id sample_id vialnum descending date;
		
	data labship;
		set labship;
		by eems_id sample_id vialnum descending date;
		if first.vialnum;
	run;
		
	
	proc sort data=labship tagsort; by pid;
	proc sort data=sig tagsort; by pid;
		
	
	data gsa_labship_vials;
		merge labship (in=inlab) sig (keep=pid esc1 esc2);
		by pid;
		if inlab;
		
		if eems_id = 'Global Screen Array' or index(upcase(eems_id),'GSA') then do;
			in_gsa_lab = 1;
			if (esc1 = 1 and esc2 = 1) or (vialnum in ('0050','0051','0052','0053','0054')) then output;
		end;
	run;
		
		
	proc sort data=labship tagsort; by sample_id vialnum descending date;
	data labship_vials;
		set labship (in=inlab) ;
		by sample_id vialnum;
		
		length EEMS_no1-EEMS_no12 $ 19 analyte_category1-analyte_category12 $ 25;
		
		array a_EEMS_no[1:12] EEMS_no1-EEMS_no12;
		array a_labdate[1:12] labdate1-labdate12;
		array a_lien[1:12] lien1-lien12;
		array a_dna_extract[1:12] dna_extract1-dna_extract12;
		array a_analyte_category[1:12] analyte_category1-analyte_category12;
		
		retain EEMS_no_count EEMS_no1-EEMS_no12 labdate1-labdate12 dna_extract1-dna_extract12 has_labship lien1-lien12 analyte_category1-analyte_category12;
		
		if first.vialnum then do;
			EEMS_no_count = 0;
			has_labship = 1;
			do i = 1 to 12;
				a_EEMS_no[i] = '';
				a_labdate[i] = .M;
				a_dna_extract[i] = 0;
				a_lien[i] = 0;
				a_analyte_category[i] = '';
			end;
		end;
		
		EEMS_no_count = EEMS_no_count + 1;
		if EEMS_no_count <= 12 then do;
			a_eems_no[EEMS_no_count] = eems_id;
			
			if not missing(date) then a_labdate[EEMS_no_count] = date;
			else a_labdate[EEMS_no_count] = .M;
			
			if not missing(amount_requested) then a_dna_extract[EEMS_no_count] = 1;
			if not missing(amount_requested) then a_lien[EEMS_no_count] = amount_requested;
			
			a_analyte_category[EEMS_no_count] = analyte_category;
		end;
		else if EEMS_no_count > 12 then put "Warning: More than 12 EEMS Studies. There are " EEMS_no_count; 
	
		if last.vialnum then output;
	run;
	
	
****************************************************************;
****************************************************************;
****************************************************************;
**************** DNA at DESL Selected for GSA ******************;
****************************************************************;
****************************************************************;
****************************************************************;
****************************************************************;

	proc sort data=gsa_labship_vials tagsort; by bsi_id;
	
	data gsa_labship_desl_dna (keep=sampleid pid bsi_id);
		set gsa_labship_vials;
		by bsi_id;	
		
		length sampleid $ 7;
		sampleid = substr(bsi_id,1,7);
		
		if missing(vialnum)	then output;
	run;
	
	proc cport data=gsa_labship_desl_dna file=gsadesl;
	

****************************************************************;
****************************************************************;
****************************************************************;
***************** Variables Needed for DESL ********************;
***************** Also Output to Masterfile ********************;
****************************************************************;
****************************************************************;
****************************************************************;
	
	**** Process Vial Information Needed to Determine DESL status variables;
	data all_vials;
		set all_vials;
		
		length temp_vcomm1-temp_vcomm15 $ 250;
		mod_num = 10;
		vcode_num = 15;
		array a_mod[1:10] mod1-mod10;
		array a_value[1:10] value1-value10;
		array a_vcode[1:15] vcode1-vcode15;	
		array a_vcomm[1:15] vcomm1-vcomm15;	
		array a_temp_vcomm[1:15] temp_vcomm1-temp_vcomm15;
		
		source_dna = .N;
		dna_conc = .N;
		staged_dna = .N;
		
		if mattype = 'CB' then do;
			source_dna = .M;
			dna_conc = .M;
			staged_dna = .M;
		end;
		
		do i = 1 to mod_num;
			if a_mod[i] = 'DNA' then dna_conc = a_value[i];
		end;
		if dna_conc >= 1000 then dna_conc = dna_conc/1000;
		
			
		bptl_update_comment = 0;
		bptl_update_month = .N;
		bptl_update_year = .N;
			
		do i = 1 to 15;			
			a_temp_vcomm[i] = a_vcomm[i];
			if index(a_temp_vcomm[i],'260/280') then a_temp_vcomm[i] = tranwrd(a_temp_vcomm[i], "260/280", "260-280");
			
			if a_vcode[i] = 'C' and index(a_temp_vcomm[i],'BPTL') then do;
				bptl_update_comment = 1;
				bptl_DNA_quant_dt = substr(a_temp_vcomm[i],index(a_temp_vcomm[i],'/')-2,7);
			end;
		end;
		
		if not missing(bptl_DNA_quant_dt) and substr(bptl_DNA_quant_dt,1,1) = ' ' then bptl_DNA_quant_dt = cat('0',substr(bptl_DNA_quant_dt,2,6));
		
		if mattype in ("CB") then do;
			if dna_usability in (1,12) then staged_dna = 1;
			else if dna_usability = 2 then source_dna = 0;
			else if dna_usability in (3,8,14) then source_dna = 1;
			
			if bptl_update_comment = 1 then source_dna = 1;

			if staged_dna = 1 then source_dna = 1;
			
			do i = 1 to vcode_num;
				if a_vcode[i] in ("STATE") then dna_state = input(a_vcomm[i],2.);
				
				*if a_vcode[i] in ("STAGED") and a_vcomm[i] = '1' then staged_dna = 1;
				
				*if a_vcode[i] in ("SOURCE") and a_vcomm[i] = '1' then source_dna = 1;
				*else if a_vcode[i] in ("STAGED") and a_vcomm[i] = '1' then source_dna = 1;
				*else if source_dna ne 1 and a_vcode[i] in ("SOURCE") and a_vcomm[i] = '0' then source_dna = 0;
			end;
		end;
		
		
		**** amount;	
		if volume = 999 then volume_mL = .M;	
		else if vol_unit = 'M' and volume > 100 then volume_mL = volume / 1000; *** for vials with milliliters and not microliters; 
		else if vol_unit = 'M' then volume_mL = volume;
		else if vol_unit = 'B' then volume_mL = volume / 1000;
		else if mattype not in ('CB','C3') then volume_mL = .M;
		else volume_mL = .N;
		
   	if vialstat='3' and volume_mL=.M and dna_mass <=1  and mattype='CB' then dna_incomplete=1;
   	dna_mass_ug = .N;
   	
		
		if mattype='CB' then do ;
			dna_mass_ug = .M;
			if not missing(dna_mass) then do;
				if dna_mass_unit = 'A' then dna_mass_ug = dna_mass; *** micrograms;
				else if dna_mass_unit = 'D' then dna_mass_ug = dna_mass * 1000; *** milligrams;
				else if dna_mass_unit = 'N' then dna_mass_ug = dna_mass/1000; *** nanograms;
				else if dna_mass_unit = 'P' then dna_mass_ug = dna_mass/1000000; *** picograms;
				else if dna_mass_unit = 'U' then dna_mass_ug = .M; *** unknown;
			end; 
			else if not missing(volume_mL) and volume_mL ne .M then do;
				if vol_unit = 'A' then dna_mass_ug = volume_mL;
				else do;
					if not missing(volume_mL) then temp_vol = volume_mL * 1000; *** milliliter to microliter;
					if not missing(dna_concentration) then temp_conc = dna_concentration;
					else if not missing(dna_conc) then temp_conc = dna_conc;
					if not missing(temp_conc) and not missing(temp_vol) then dna_mass_ug = (temp_conc * temp_vol)/1000; 
					else dna_mass_ug = .M;
				end;
			end;
	  end;
	run;
	
	
		
****************************************************************;
****************************************************************;
****************************************************************;
****************** DESL File Implementation ********************;
****************************************************************;
****************************************************************;
****************************************************************;
****************************************************************;

  proc sort data=desl; by bsi_sampleid;  
	**** set desl to one record per sampleid - all records per sampleid match;
	
	data desl (keep=sampleid in_desl desl_sss desl_coldroom desl_stat masked_desl_sampleid bsi_sampleid);
		set desl (rename=(masked_desl_sampleid= raw_masked_desl_sampleid)); 
		by bsi_sampleid;
		
		length sampleid $ 8 masked_desl_sampleid $ 12;
		retain sampleid desl_count in_desl desl_sss desl_coldroom sss1-sss8 coldroom1-coldroom8 desl_stat1-desl_stat8 masked_desl_sampleid;
		array a_sss[1:8] sss1-sss8;
		array a_coldroom[1:8] coldroom1-coldroom8;
		array a_desl_stat[1:8] desl_stat1-desl_stat8; 
		
		if first.bsi_sampleid then do;
			sampleid = bsi_sampleid;
			desl_count = 0;
			in_desl = .N;
			desl_sss = .N;
			desl_coldroom = .N;
			masked_desl_sampleid = '';
			do i =1 to 8;
				a_sss[i] = .N;
				a_coldroom[i] = .N;
				a_desl_stat[i] = .N;
			end;
		end;
		
		desl_count = desl_count + 1;
		a_sss[desl_count] = sss;
		a_coldroom[desl_count] = cold_room;
		a_desl_stat[desl_count] = desl_stat;
		masked_desl_sampleid = raw_masked_desl_sampleid;
			
		if last.bsi_sampleid then do;
			if desl_stat = 1 then in_desl = 1;
			else in_desl = 0;
			
			desl_sss = sss;
			desl_coldroom = cold_room;
			
			output;
		end;
	run;
  
 
  *** find child vials;
	proc sort data=all_vials tagsort; by sampleid seq_num;
	data child_wparent child_orphan;
		set all_vials (keep= sampleid seq_num vialstat mattype reposid dt_mod source_id parent volume volume_mL dna_mass_ug);
		by sampleid seq_num;
		if mattype = 'CB' and (seq_num < '2700' or seq_num > '2799');
		
		length source_seq $ 4 source_sampleid $ 8;
		if missing(source_id) then source_id = parent;
		
		source_seq = substr(source_id,9,4);
		source_sampleid = substr(source_id,1,8);
		if not missing(source_id) and (substr(source_id,9,4) > "0113" and not (substr(source_id,9,4) in ('0133','0134') and mattype = 'CC')) then output child_orphan;
		else if not missing(source_id) and substr(source_id,9,4) ne '0024' and (substr(source_id,9,4) <='0113' or (substr(source_id,9,4) in ('0133','0134') and mattype = 'CC')) then output child_wparent;
	run;
	
	proc sort data=child_orphan tagsort; by source_sampleid source_seq;
	data child_wparent2 child_orphan2;
		merge all_vials (keep= sampleid seq_num vialstat mattype reposid dt_mod source_id parent volume) 
					child_orphan (in=inorphan keep= source_sampleid source_seq rename=(source_sampleid = sampleid source_seq = seq_num));
		by sampleid seq_num;
		if inorphan;
		
		length source_seq $ 4 source_sampleid $ 8;
		if missing(source_id) then source_id = parent;
		source_seq = substr(source_id,9,4);
		source_sampleid = substr(source_id,1,8);
		
		if not missing(source_id) and substr(source_id,9,4) > "0113" and not (substr(source_id,9,4)  in ('0133','0134') and mattype = 'CC') then output child_orphan2;
		else if not missing(source_id) and substr(source_id,9,4) ne '0024' and (substr(source_id,9,4) <='0113' or (substr(source_id,9,4) in ('0133','0134') and mattype = 'CC')) then output child_wparent2;
	run;
			
	proc sort data=child_orphan2 tagsort; by source_sampleid source_seq;
	data child_wparent3 child_orphan3;
		merge all_vials (keep= sampleid seq_num vialstat mattype reposid dt_mod source_id parent volume) 
					child_orphan2 (in=inorphan keep= source_sampleid source_seq rename=(source_sampleid = sampleid source_seq = seq_num));
		by sampleid seq_num;
		if inorphan;
		
		length source_seq $ 4 source_sampleid $ 8;
		if missing(source_id) then source_id = parent;
		source_seq = substr(source_id,9,4);
		source_sampleid = substr(source_id,1,8);
		
		if not missing(source_id) and substr(source_id,9,4) > "0113" and not (substr(source_id,9,4) in ('0133','0134') and mattype = 'CC') then output child_orphan3;
		else if not missing(source_id) and substr(source_id,9,4) ne '0024' and (substr(source_id,9,4) <='0113' or (substr(source_id,9,4) in ('0133','0134') and mattype = 'CC')) then output child_wparent3;
	run;

	proc sort data=child_wparent tagsort; by source_sampleid source_seq;
	proc sort data=child_wparent2 tagsort; by source_sampleid source_seq;
	proc sort data=child_wparent3 tagsort; by source_sampleid source_seq;
	
	**** determine DESL factors;
	data child_wparent; 
		set child_wparent;
		by source_sampleid source_seq;
		if last.source_seq;
	run;
	data child_wparent2; 
		set child_wparent2;
		by source_sampleid source_seq;
		if last.source_seq;
	run;
	data child_wparent3; 
		set child_wparent3;
		by source_sampleid source_seq;
		if last.source_seq;
	run;
	
	data child_wparent;
		merge child_wparent
				child_wparent2
				child_wparent3;
		by source_sampleid source_seq;
		
		retain has_desl has_other avail_desl avail_other dna_mod dna_dt_mod dna_volume dna_vialstat;
		
		if first.source_seq then do;
			has_desl = 0;
			has_other = 0;
			avail_desl = 0;
			avail_other = 0;
			dna_mod = 0;
			dna_dt_mod = .N;
			dna_volume = .N;
			dna_vialstat = '   ';
		end;
		
		test_seq = substr(source_id,9,4);
		if reposid = 'F' then do;
			has_desl = has_desl + 1;
			if vialstat in ('1','3') and volume_mL not in (.M,999) and (volume_mL >= .1 or dna_mass_ug >= .2) then do;
				avail_desl = avail_desl + 1;	
				dna_dt_mod = dt_mod;
				dna_volume = dna_mass_ug;
				dna_vialstat = vialstat;
			end;
			if dt_mod > mdy (3,14,2014) then dna_mod = 1;
		end;
		else do;
			has_other = has_other + 1;
			if vialstat in ('1','3') and volume_mL not in (.M,999) and (volume_mL >= .1 or dna_mass_ug >= .2) then avail_other = avail_other + 1;	
		end;
		
		if last.source_seq then output;
	run;
		
		
	
	proc sort data=all_vials tagsort; by sampleid seq_num;
	proc sort data=labship tagsort; by sample_id vialnum descending date;	
	
	*** determine labship factors;
	data labship_desl;
		set labship (keep= sample_id vialnum date rename=(sample_id = sampleid date = labdate vialnum = seq_num)); 
		by sampleid seq_num;

		retain lab_after;
		if first.seq_num then lab_after = 0;
		
		if labdate > mdy (1,1,2014) then lab_after = 1;
		
		if last.seq_num then output;
	run;
		
	
		
	**** bring in labship and source vials;
	data bsi_vials;
		merge all_vials (in=inspecimen keep= bsi_id sampleid seq_num vialstat mattype reposid dt_mod volume vol_unit dna_mass_ug volume_mL)
					labship_desl (keep=sampleid seq_num lab_after);
		by sampleid seq_num;
		if inspecimen;
		if (mattype in ('CC','CS','CH','B4') and seq_num ne '0024' and (seq_num <='0113' or (seq_num in ('0133','0134') and mattype = 'CC'))) or (mattype = 'CB' and (seq_num < '2700' or seq_num > '2799'));
		
		if mattype in ('CC','CS','CH','B4') and seq_num ne '0024' and (seq_num <='0113' or (seq_num in ('0133','0134') and mattype = 'CC')) then do;
			is_source = 1;
	 		
	 		if vialstat in ('3','7') then vstat = 1; *** reserved/pending;
	 		else if vialstat in ('2','4','5','6') then vstat = 2;
	 		else if ((mattype = 'CC' and 0 <= volume_mL < .7) or (mattype = 'CS' and 0 <= volume_mL < 1.5)) then vstat = 2; *** out/empty;
	 		else if vialstat in ('1') then do; *** in half or in full;
	 			if (mattype = 'CC' and .7 <= volume_mL < 1.5 ) then vstat = 3; ** in half;
	 			else if (mattype = 'CS' and 1.5 <= volume_mL < 3) then vstat = 3; ** in half;
	 			else if (vialstat in ('1') and volume_mL > 1.8) or (vialstat = '1' and mattype = 'B4') or (vialstat = '1' and mattype = 'CB') or (mattype = 'CC' and volume_mL > 1.5 ) or (mattype = 'CH') 
	 				then vstat = 4;  **** in full;
	 		end;
	 	end;
	run;	
		
	***** merge source information with child dna information;
	data bsi_vials;
		merge bsi_vials 
					child_wparent (in=inchild keep= source_sampleid source_seq has_desl has_other avail_desl avail_other dna_mod dna_dt_mod dna_volume dna_vialstat rename= (source_sampleid=sampleid source_seq = seq_num));
		by sampleid seq_num;
			
		if not inchild then dna_mod = .N;
		if not inchild then dna_rep = 1;
		else if has_desl > 0 and has_other > 0 then do;
			if avail_desl > 0 and avail_other > 0 then dna_rep = 2;
			else if avail_desl > 0 and avail_other = 0 then dna_rep = 3;
			else if avail_desl = 0 and avail_other > 0 then dna_rep = 4;
			else if avail_desl = 0 and avail_other = 0 then dna_rep = 5;
		end;
		else if has_desl > 0 and has_other = 0 then do;
			if avail_desl > 0 then dna_rep = 6;
			else if avail_desl = 0 then dna_rep = 7;
		end;
		else if has_desl = 0 and has_other > 0 then do;
			if avail_other > 0 then dna_rep = 8;
			else if avail_other = 0 then dna_rep = 9;
		end;
	run;
	
	
	*** bring DESL and BSI together;
	proc sort data=desl tagsort; by sampleid;
	data bsi_vials;
		merge bsi_vials (in=inbsi)
					desl (in=indesl keep= sampleid in_desl);
		by sampleid;
		if inbsi;
		
		if not indesl then in_desl = .N;
	run;
		
	*** determine DNA factors;
	data dna_vials;
		set bsi_vials;
		by sampleid;
		if (mattype = 'CB' and (seq_num < '2700' or seq_num > '2799'));
		
		if vialstat in ('1','3') and volume_mL not in (.M,999) and (volume_mL >= .1 or dna_mass_ug >= .2) then avail_dna = 1;
		else avail_dna = 0;
		
		if dt_mod > mdy (3,14,2014) then dna_mod_all = 1;
		else dna_mod_all = 0;
	run;
				
	**** desl information on sampleid level;
	data dna_mods_sampleid;
		set all_vials (keep=sampleid mattype seq_num reposid dt_mod);
		by sampleid;
		where (mattype in ('CC','CS','CH','B4') and seq_num ne '0024' and (seq_num <='0113' or (seq_num in ('0133','0134') and mattype = 'CC'))) or (mattype = 'CB' and (seq_num < '2700' or seq_num > '2799'));
		
		retain dna_mod_sampleid;
		
		if first.sampleid then dna_mod_sampleid = 0;
		
		if mattype = 'CB' and reposid = 'F' and dt_mod > mdy (3,14,2014) then dna_mod_sampleid = 1;
		
		if last.sampleid then output;
	run;
	
	**** bring DESL and sample level together;
	data sampleid_vials;
		merge desl (in=inbsi keep=sampleid in_desl masked_desl_sampleid desl_sss desl_coldroom desl_stat) dna_mods_sampleid (in=inmod keep=sampleid dna_mod_sampleid);
		by sampleid;	
		if inmod;
		
		if not inmod then dna_mod_sampleid = .N;
		if not inbsi then in_desl = .N;
		
	run;
	
  
	

****************************************************************;
****************************************************************;
****************************************************************;
************* Sample IDs with No DNA from DESL  ****************;
****************************************************************;
****************************************************************;
****************************************************************;
****************************************************************;

	proc sort data=unusedna tagsort; by sampleid last_date_known_unusable;
	proc sort data=all_vials tagsort; by sampleid;
	
	proc freq data=unusedna;
		table status /list missing;
	run;
	
	data unusedna;
		set unusedna;
		by sampleid;
		
		in_desl_flagged_returns = .N;
		if missing(status) or index(upcase(status),'FAIL CONCENTRATION') or index(upcase(status),'PROTECTED') or index(upcase(status),'INSUFFICIENT DNA') or index(upcase(status),'SUFFICIENT DNA') 
			or index(upcase(status),'SKIPPED EXTRACTION TO PRESERVE') or index(upcase(status),'FAILED EXTRACTION < 750UL IN S') or index(upcase(status),'EXTRACT ONLY') 
			or index(upcase(status),'LOW CALL RATE') or index(upcase(status),'FAILED STAGING') or index(upcase(status),'MICROBIOME') or index(upcase(status),'CONTAMINATED') 
			or index(upcase(status),'FAILED GWAS') or index(upcase(status),'FAILED INITIAL EXTRACTION') or index(upcase(status),'DEPLETED')
			then in_desl_flagged_returns = 1;
		else if index(upcase(status),'NOT AVAILABLE') or index(upcase(status),'QUARANTINE') or index(upcase(status),'DISCORDANT') or index(upcase(status),'NOT FOUND') 
		 or index(upcase(status),'NOT AT CGR/DESL') or index(upcase(status),'NEVER PROCESSED')
			then in_desl_flagged_returns = 0;
		else put "ERROR: Unaccounted for Useable DNA Status:  " status;
	run;
	
	data unusedna;
		set unusedna;
		by sampleid last_date_known_unusable;
		if last.sampleid;
	run;

		
	data all_vials;
		merge unusedna (in=inused keep=sampleid last_date_known_unusable)
					all_vials (in=invial);
		by sampleid;
		if invial;
		
		has_unusable_dna_dt = inused;
	run;
	
	
****************************************************************;
****************************************************************;
****************************************************************;
********************** Requisition Data ************************;
****************************************************************;
****************************************************************;
****************************************************************;
****************************************************************;

proc sort data=reqdata tagsort; by bsi_id;
proc sort data=all_vials tagsort; by bsi_id;

proc contents data=reqdata;

data reqin;
	set reqdata (rename=(num_vials = req_num_vials));
	if not missing(req_dt_req) and not missing(task_id);
run;	

data reqin;
	merge reqin (in=inreq)
				all_vials (keep=bsi_id reposid vialstat rename=(reposid = bmf_reposid vialstat = bmf_vialstat));
	by bsi_id;
	if inreq;
run;


	


proc sort data=reqin tagsort; by requisition_id bsi_id;
	data req_vial;
		set reqin;
		by requisition_id bsi_id;
		if not missing(task_name) or not missing(rvt_status) or not missing(task_type);
		
		length req_last_reposid $ 3;
		
		retain num_tasks complete_any pend_any needs_status aliquot_status num_aliquot return_status num_return discard_status num_discard ship_status num_ship transfer_status num_transfer process_dna_status num_process_dna
					 process_status num_process pool_status num_pool relabel_status relabel_num ebv_status ebv_num missing_status missing_num investigate_status num_investigate pull_status num_pull scan_status num_scan
					 extract_status extract_num receive_status num_receive quant_status num_quant test_status num_test stage_status num_stage review_status num_review qc_status num_qc confirm_status num_confirm
					 hold_status num_hold assay_status num_assay prelim_status num_prelim ims_status num_ims boxorder_status num_boxorder childsource_status num_childsource modify_status num_modify verify_status num_verify
					 owner_status num_owner revision_status num_revision blind_status num_blind update_status num_update 
					 transfer_destination transfer_out2 task_dt_102016 year_task_mod req_created_child req_last_reposid;
		
		if first.bsi_id then do;
			num_tasks = 0;
			complete_any = 0;
			pend_any = 0;
			
			needs_status = 0;
			aliquot_status = 0;
			num_aliquot = 0;
			return_status = 0;
			num_return = 0;
			discard_status = 0;
			num_discard = 0;
			ship_status = 0;
			num_ship = 0;
			transfer_status = 0;
			num_transfer = 0;
			process_dna_status = 0;
			num_process_dna = 0;
			process_status = 0;
			num_process = 0;
			pool_status = 0;
			num_pool = 0;
			relabel_status = 0;
			relabel_num = 0;
			ebv_status = 0;
			ebv_num = 0;
			missing_status = 0;
			missing_num = 0;
			investigate_status = 0;
			num_investigate = 0;
			pull_status = 0;
			num_pull = 0;
			scan_status = 0;
			num_scan = 0;
			extract_status = 0;
			extract_num = 0;
			receive_status = 0;
			num_receive = 0;
			quant_status = 0;
			num_quant = 0;
			test_status = 0;
			num_test = 0;
			stage_status = 0;
			num_stage = 0;
			review_status = 0;
			num_review = 0;
			qc_status = 0;
			num_qc = 0;
			confirm_status = 0;
			num_confirm = 0;
			hold_status = 0;
			num_hold = 0;
			assay_status = 0;
			num_assay = 0;
			prelim_status = 0;
			num_prelim = 0;
			ims_status = 0;
			num_ims = 0;
			boxorder_status = 0;
			num_boxorder = 0;
			childsource_status = 0;
			num_childsource = 0;
			modify_status = 0;
			num_modify = 0;
			verify_status = 0;
			num_verify = 0;
			owner_status = 0;
			num_owner = 0;
			revision_status = 0;
			num_revision = 0;
			blind_status = 0;
			num_blind = 0;
			update_status = 0;
			num_update = 0;
			
			transfer_destination = .N;
			transfer_out2 = 0;
			task_dt_102016 = 0;
			year_task_mod = 0;
			
			req_created_child = 0;
			req_last_reposid = '';
		end;
		
		num_tasks = num_tasks + 1;
		
		if rvt_status in (1,2) then complete_any = 1;
		else pend_any = 1;		
		
		task_name = upcase(task_name);
		
		if task_type in ("A") or index(task_name,'ALIQUOT') or not missing(rv_parent) then do;
			num_aliquot = num_aliquot + 1;
			if not missing(rv_parent) and missing(rvt_status) then aliquot_status = 3;
			else if rvt_status not in (1,2) and aliquot_status not in (2,3) then  aliquot_status = 1;
			else if rvt_status in (1,2) and bmf_vialstat ne '6' and aliquot_status not in (3) then aliquot_status = 2;
			else if rvt_status in (1,2) and bmf_vialstat = '6' then aliquot_status = 3;
		end;
		else if task_type in ('BR','F','M','Y') then do;
			num_return = num_return + 1;
			if rvt_status not in (1,2) and return_status not in (3) then return_status = 1;
			else if rvt_status in (1,2) then return_status = 3;
		end;
		else if task_type in ('B','D','E','L') or index(task_name,'DISCARD') then do;
			num_discard = num_discard + 1;
			if rvt_status not in (1,2) and discard_status not in (2,3) then discard_status = 1;
			else if rvt_status in (1,2) and bmf_vialstat not in ('4','5','6') and discard_status not in (3) then discard_status = 2;
			else if rvt_status in (1,2) and bmf_vialstat in ('4','5','6')  then discard_status = 3;
		end;
		else if task_type in ('S') then do;
			num_ship = num_ship + 1;
			if rvt_status not in (1,2) and ship_status not in (2,3) then ship_status = 1;
			else if rvt_status in (1,2) and bmf_vialstat not in ('2') and ship_status not in (2) then ship_status = 2;
			else if rvt_status in (1,2) and bmf_vialstat in ('2')  then ship_status = 3;
		end;
		else if task_type in ('X','BT') then do;
			num_transfer = num_transfer + 1;
			if rvt_status not in (1,2) and transfer_status not in (2,3) then transfer_status = 1;
			else if rvt_status in (1,2) and bmf_vialstat not in ('2') and transfer_status not in (3) then transfer_status = 2;
			else if rvt_status in (1,2) and bmf_vialstat in ('2')  then transfer_status = 3;
			transfer_destination = TASK_DEST_ID;
			if lowcase(task_name) = 'transfer out:2' then transfer_out2 = transfer_out2 + 1;
		end;
		else if index(task_name,'PROCESS DNA') then do;
			num_process_dna = num_process_dna + 1;
			if rvt_status not in (1,2) and process_dna_status not in (2,3)  then process_dna_status = 1;
			else if rvt_status in (1,2)  then process_dna_status = 3;
		end;
		else if index(task_name,'POOL') or index(task_name,'MIX VIAL') then do;
			num_pool = num_pool + 1;
			if rvt_status not in (1,2) and pool_status not in (2,3) then pool_status = 1;
			else if rvt_status in (1,2)  then pool_status = 3;
		end;
		else if index(task_name,'PROCESS') or task_type = 'P' then do;
			num_process = num_process + 1;
			if rvt_status not in (1,2) and process_status not in (2,3) then process_status = 1;
			else if rvt_status in (1,2)  then process_status = 3;
		end;
		else if index(task_name,'RELABEL') then do;
			num_relabel = num_relabel + 1;
			if rvt_status not in (1,2)  and relabel_status not in (2,3)  then relabel_status = 1;
			else if rvt_status in (1,2) then relabel_status = 3;
		end;
		else if index(task_name,'LABEL') or task_type = 'I' then do;
			num_relabel = num_relabel + 1;
			if rvt_status not in (1,2) and relabel_status not in (2,3) then relabel_status = 1;
			else if rvt_status in (1,2)  then relabel_status = 3;
		end;
		else if index(task_name,'EBV') then do;
			ebv_num = ebv_num + 1;
			if rvt_status not in (1,2)  and ebv_status not in (2,3)  then ebv_status = 1;
			else if rvt_status in (1,2) then ebv_status = 3;
		end;
		else if task_type = 'N' then do;
			num_investigate = num_investigate + 1;
			if rvt_status not in (1,2) and investigate_status not in (2,3) then investigate_status = 1;
			else if rvt_status in (1,2)  then investigate_status = 3;
		end;
		else if task_type = 'U' then do;
			num_pull = num_pull + 1;
			if rvt_status not in (1,2) and pull_status not in (2,3) then pull_status = 1;
			else if rvt_status in (1,2)  then pull_status = 3;
		end;
		else if index(task_name,'SCAN') then do;
			num_scan = num_scan + 1;
			if rvt_status not in (1,2) and scan_status not in (2,3) then scan_status = 1;
			else if rvt_status in (1,2)  then scan_status = 3;
		end;
		else if index(task_name,'EXTRACT') then do;
			num_extract = num_extract + 1;
			if rvt_status not in (1,2) and extract_status not in (2,3) then extract_status = 1;
			else if rvt_status in (1,2)  then extract_status = 3;
		end;
		else if index(task_name,'RECEIVE') or index(task_name,'RECEIPT') then do;
			num_receive = num_receive + 1;
			if rvt_status not in (1,2) and receive_status not in (2,3) then receive_status = 1;
			else if rvt_status in (1,2)  then receive_status = 3;
		end;
		else if index(task_name,'QUANT') or index(task_name,'QUNAT') or index(task_name,'QUANIT') then do;
			num_quant = num_quant + 1;
			if rvt_status not in (1,2) and quant_status not in (2,3) then quant_status = 1;
			else if rvt_status in (1,2)  then quant_status = 3;
		end;
		else if index(task_name,'TEST') then do;
			num_test = num_test + 1;
			if rvt_status not in (1,2)  and test_status not in (2,3) then test_status = 1;
			else if rvt_status in (1,2) then test_status = 3;
		end;
		else if index(task_name,'STAGE') then do;
			num_stage = num_stage + 1;
			if rvt_status not in (1,2) and stage_status not in (2,3) then stage_status = 1;
			else if rvt_status in (1,2)  then stage_status = 3;
		end;
		else if index(task_name,'REVIEW') then do;
			num_review = num_review + 1;
			if rvt_status not in (1,2) and review_status not in (2,3) then review_status = 1;
			else if rvt_status in (1,2)  then review_status = 3;
		end;
		else if index(task_name,'QC') then do;
			num_qc = num_qc + 1;
			if rvt_status not in (1,2) and qc_status not in (2,3) then qc_status = 1;
			else if rvt_status in (1,2)  then qc_status = 3;
		end;
		else if index(task_name,'CONFIRM') then do;
			num_confirm = num_confirm + 1;
			if rvt_status not in (1,2) and confirm_status not in (2,3) then confirm_status = 1;
			else if rvt_status in (1,2) then confirm_status = 3;
		end;
		else if task_type = 'H' then do;
			num_hold = num_hold + 1;
			if rvt_status not in (1,2) and hold_status not in (2,3) then hold_status = 1;
			else if rvt_status in (1,2)  then hold_status = 3;
		end;
		else if index(task_name,'ASSAY') then do;
			num_assay = num_assay + 1;
			if rvt_status not in (1,2) and assay_status not in (2,3) then assay_status = 1;
			else if rvt_status in (1,2)  then assay_status = 3;
		end;
		else if index(task_name,'PRELIM') then do;
			num_prelim = num_prelim + 1;
			if rvt_status not in (1,2) and prelim_status not in (2,3) then prelim_status = 1;
			else if rvt_status in (1,2)  then prelim_status = 3;
		end;
		else if index(task_name,'IMS') then do;
			num_ims = num_ims + 1;
			if rvt_status not in (1,2) and ims_status not in (2,3) then ims_status = 1;
			else if rvt_status in (1,2)  then ims_status = 3;
		end;
		else if index(task_name,'BOX ORDER') then do;
			num_boxorder = num_boxorder + 1;
			if rvt_status not in (1,2) and boxorder_status not in (2,3) then boxorder_status = 1;
			else if rvt_status in (1,2)  then boxorder_status = 3;
		end;
		else if index(task_name,'CHILD SOURCE') then do;
			num_childsource = num_childsource + 1;
			if rvt_status not in (1,2) and childsource_status not in (2,3) then childsource_status = 1;
			else if rvt_status in (1,2)  then childsource_status = 3;
		end;
		else if task_type = '5' then do;
			num_modify = num_modify + 1;
			if rvt_status not in (1,2) and modify_status not in (2,3) then modify_status = 1;
			else if rvt_status in (1,2)  then modify_status = 3;
		end;
		else if task_type = '6' or index(task_name,'VERIF') then do;
			num_verify = num_verify + 1;
			if rvt_status not in (1,2) and verify_status not in (2,3) then verify_status = 1;
			else if rvt_status in (1,2)  then verify_status = 3;
		end;
		else if task_type = '4' then do;
			num_owner = num_owner + 1;
			if rvt_status not in (1,2) and owner_status not in (2,3) then owner_status = 1;
			else if rvt_status in (1,2)  then owner_status = 3;
		end;
		else if task_type = 'T' then do;
			num_revision = num_revision + 1;
			if rvt_status not in (1,2) and revision_status not in (2,3) then revision_status = 1;
			else if rvt_status in (1,2)  then revision_status = 3;
		end;
		else if task_type = 'J' then do;
			num_blind = num_blind + 1;
			if rvt_status not in (1,2) and blind_status not in (2,3) then blind_status = 1;
			else if rvt_status in (1,2)  then blind_status = 3;
		end;
		else if index(task_name,'UPDATE') then do;
			num_update = num_update + 1;
			if rvt_status not in (1,2) and update_status not in (2,3) then update_status = 1;
			else if rvt_status in (1,2)  then update_status = 3;
		end;  
		else if missing(task_type) and missing(task_name) then do;
			missing_num = missing_num + 1;
			if rvt_status not in (1,2) and missing_status not in (2,3) then missing_status = 1;
			else if rvt_status in (1,2)  then missing_status = 3;
		end;
		else needs_status = needs_status + 1;
			 

		if not missing(task_dt_ls_mod) then do;
			if year(task_dt_ls_mod) > year_task_mod then year_task_mod = year(task_dt_ls_mod);
			
			if task_dt_ls_mod < mdy(10,01,2016) and task_dt_102016 ne 2 then task_dt_102016 = 1;
			else task_dt_102016 = 2;
		end;
		
		if not missing(rv_parent) then req_created_child = 1;
		if rvt_reposid ^= 'E' then req_last_reposid = rvt_reposid;
		
		if last.bsi_id then do;
			vial_req_status = 0;
			vial_req_reason = 0;
			
			req_year = substr(requisition_id,2,4);
			
			if req_dt_ls_mod < mdy(10,01,2016) then req_dt_102016 = 1;
			else req_dt_102016 = 2;
			
			if req_dt_req < mdy(10,01,2016) then req_dt_req_102016 = 1;
			else req_dt_req_102016 = 2;
			
			if missing(req_last_reposid) then req_last_reposid = rvt_reposid;
			
			if return_status in (2,3) and req_dt_req_102016 = 1 then do;
				vial_req_status = 1;
				vial_req_reason = 1;
			end;
			else if return_status in (2,3) and verify_status in (2,3) and req_dt_req_102016 = 2 then do;
				vial_req_status = 1;
				vial_req_reason = 2;
			end;
			else if return_status in (2,3) and verify_status in (0) and req_dt_req_102016 = 2 then do;
				vial_req_status = 1;
				vial_req_reason = 2;
			end;
			else if ship_status in (2,3) then do;
				vial_req_status = 1;
				vial_req_reason = 3;
			end;
			else if discard_status in (2,3) then do;
				vial_req_status = 1;
				vial_req_reason = 4;
			end;
			else if aliquot_status in (1) then do;
				vial_req_status = 2;
				vial_req_reason = 5;
			end;
			else if aliquot_status in (2,3) then do;
				vial_req_status = 1;
				vial_req_reason = 5;
			end;
			else if return_status in (1) or (return_status in (2,3) and verify_status in (1) and req_dt_req_102016 = 2) then do;
				vial_req_status = 2;
				vial_req_reason = 6;
			end; 
			else if transfer_status in (1,2,3) then do;
				if intnx('year',req_dt_ls_mod,3,'sameday') < today() then vial_req_status = 1;
				else vial_req_status = 2;
				vial_req_reason = 7;
			end;
			else if process_status in (1,2,3) then do;
				if intnx('year',req_dt_ls_mod,3,'sameday') < today() then vial_req_status = 1;
				else vial_req_status = 2;
				vial_req_reason = 8;
			end;
			else do;
				vial_req_status = 2;
				vial_req_reason = 9;
			end;
			output;
		end;		
	run;	
			
	proc sort data=req_vial tagsort; by requisition_id bsi_id;
	data reqs;
		set req_vial (rename= (transfer_destination=vial_transfer_destination year_task_mod= vial_year_task_mod));
		by requisition_id bsi_id;
		
		retain req_status req_reason num_vials num_complete num_pending returned aliquoted discarded shipped transferred other pending_return process atcc latest_mod transfer_destination pending_transfer2
					 year_task_mod req_pend_any;
		
		if first.requisition_id then do;
			req_status = 0;
			req_reason = 0;
			num_vials = 0;
			num_complete = 0;
			num_pending = 0;
			returned = 0;
			aliquoted = 0;
			discarded = 0;
			shipped = 0;
			transferred = 0;
			other = 0;
			pending_return = 0;
			process = 0;
			atcc = 0;
			latest_mod = .N;
			transfer_destination= .N;
			pending_transfer2 = 0;
			year_task_mod = vial_year_task_mod;
			req_pend_any = 0;
		end;
		
		if not missing(req_dt_ls_mod) and req_dt_ls_mod > latest_mod then latest_mod = req_dt_ls_mod;
		if not missing(vial_transfer_destination) then transfer_destination = vial_transfer_destination;
		if not missing(vial_year_task_mod) and year_task_mod > vial_year_task_mod then year_task_mod = vial_year_task_mod;
		
		if pend_any = 1 then req_pend_any = pend_any;
		
		if first.bsi_id then num_vials = num_vials + 1;
		if vial_req_status = 1 then num_complete = num_complete + 1;
		if vial_req_status = 2 then num_pending = num_pending + 1;		
	
		if vial_req_reason = 1 and vial_req_status = 1 then returned = returned + 1;
		if vial_req_reason = 2 and vial_req_status = 1 then returned = returned + 1;
		if vial_req_reason = 3 and vial_req_status = 1 then shipped = shipped + 1;
		if vial_req_reason = 4 and vial_req_status = 1 then discarded = discarded + 1;
		if vial_req_reason = 5 and vial_req_status = 1 then aliquoted = aliquoted + 1;
		if vial_req_reason = 6 then pending_return = pending_return + 1;
		if vial_req_reason = 7 and vial_req_status = 1 then transferred = transferred + 1;
		if vial_req_reason = 8 and vial_req_status = 1 then process = process + 1;
		if vial_req_reason = 9 and vial_req_status = 1 then other = other + 1;
		
		
		if bmf_vialstat in ('1','3','4','5','6') and bmf_reposid = 'E' then atcc = atcc + 1;
		
		pct_complete = round(((sum(returned,shipped,discarded,aliquoted))/num_vials)*100,.2);
		
		if last.requisition_id then do;
			if transfer_out2 = num_vials then pending_transfer2 = 1;
			else if transfer_out2 > 0 then pending_transfer2 = 2;
			
			year_mod = year(req_dt_ls_mod);
			req_status = 0;
			req_reason = 0;
			
			if num_vials = returned then do;
				req_status = 1;
				req_reason = 1;
			end;
			else if num_vials = shipped then do;
				req_status = 1;
				req_reason = 2;
			end;
			else if num_vials = discarded then do;
				req_status = 1;
				req_reason = 3;
			end;
			else if num_vials = sum(returned, aliquoted) then do;
				req_status = 1;
				req_reason = 4;
			end;
			else if num_vials = sum(returned, shipped) then do;
				req_status = 1;
				req_reason = 5;
			end;
			else if num_vials = sum(returned, discarded, shipped) then do;
				req_status = 1;
				req_reason = 6;
			end;
			else if num_vials = sum(aliquoted, shipped) then do;
				req_status = 1;
				req_reason = 7;
			end;
			else if num_vials = sum(returned, aliquoted, discarded) then do;
				req_status = 1;
				req_reason = 8;
			end;
			else if num_vials = sum(shipped, aliquoted, discarded) then do;
				req_status = 1;
				req_reason = 9;
			end;
			else if num_vials = sum(shipped, aliquoted, discarded, returned) then do;
				req_status = 1;
				req_reason = 10;
			end;
			else if today() >= intnx('year',req_dt_ls_mod,7,'sameday') and num_pending = 0 then do;
				req_status = 1;
				req_reason = 11; *** requisition hasn't been touched in 7 years and no pending tasks, nothing further expected;
			end;
			else if transferred > 0 and returned = 0 and aliquoted = 0 and discarded = 0 and shipped = 0 then do;
				req_status = 2;
				req_reason = 21;
			end;
			else if transferred > 0 and returned > 0 and num_vials = sum(transferred, returned) then do;
				req_status = 2;
				req_reason = 22;
			end;
			else if transferred > 0 and aliquoted > 0 and num_vials = sum(transferred, aliquoted) then do;
				req_status = 2;
				req_reason = 23;
			end;
			else if transferred > 0 and shipped > 0 and num_vials = sum(transferred, shipped) then do;
				req_status = 2;
				req_reason = 24;
			end;
			else if transferred > 0 and discarded > 0 and num_vials = sum(transferred, discarded) then do;
				req_status = 2;
				req_reason = 25;
			end;
			else if num_vials = sum(transferred, returned, discarded) then do;
				req_status = 2;
				req_reason = 26;
			end;
			else if num_vials = sum(transferred, returned, shipped) then do;
				req_status = 2;
				req_reason = 27;
			end;
			else if num_vials = sum(transferred, returned, aliquoted) then do;
				req_status = 2;
				req_reason = 28;
			end;
			else if num_vials = sum(transferred, aliquoted, discarded) then do;
				req_status = 2;
				req_reason = 29;
			end;
			else if num_vials = sum(transferred, aliquoted, shipped) then do;
				req_status = 2;
				req_reason = 30;
			end;
			else if num_vials = sum(transferred, discarded, shipped) then do;
				req_status = 2;
				req_reason = 31;
			end;
			else if num_vials = sum(aliquoted, other) then do;
				req_status = 2;
				req_reason = 32;
			end;
			else if transferred > 0 and other > 0 then do;
				req_status = 2;
				req_reason = 33;
			end;
			else if other > 0 then do;
				req_status = 2;
				req_reason = 34;
			end;
			else if process > 0 then do;
				req_status = 2;
				req_reason = 35;
			end;
			else do;
				req_status = 2;
				req_reason = 36;
			end;
			
			if req_status = 2 then do;
				if year(today()) - year(req_dt_ls_mod) >= 5 then req_status = 3;
			end;
			
			if latest_mod < mdy(01,01,2015) then worked_on = 1;
			else worked_on = 2;
			
			output;
		end;
	run;	
	
	
	
	
	proc sort data=req_vial tagsort; by requisition_id;
	proc sort data=reqs tagsort; by requisition_id;
	data all_reqs (rename= (req_dt_ls_mod = req_dt_mod req_dt_req = req_dt_submitted vial_req_status = req_vial_status vial_req_reason = req_vial_reason));
		length req_investigators $ 50;
		merge req_vial (keep=requisition_id bsi_id vial_req_status vial_req_reason req_created_child req_last_reposid)
					reqs (keep=requisition_id req_status req_reason req_dt_ls_mod req_year req_investigators req_dt_req req_user_id req_reason  rename=(req_user_id = bsi_req_user_id));
		by requisition_id;
		
		if missing(req_dt_submitted) then req_dt_submitted = .M;
		
		if bsi_req_user_id = 220 then req_user_id = 1; ** matt moore;
		else if bsi_req_user_id = 931 then req_user_id = 2; ** shannon merkle;
		else if bsi_req_user_id = 1376 then req_user_id = 3; ** laura hawkins;
		else if bsi_req_user_id = 1472 then req_user_id = 4; ** mike furr;
		else if bsi_req_user_id = 1499 then req_user_id = 5; ** ryan noble;
		else if bsi_req_user_id = 1721 then req_user_id = 6; ** beth levitt;
		else if bsi_req_user_id = 35000518 then req_user_id = 7; ** chris cunningham;
		else if bsi_req_user_id = 35000535 then req_user_id = 8; ** ozzarah tabazz;
		else if bsi_req_user_id = 314 then req_user_id = 10; ** karen petitt;
		else if bsi_req_user_id = 923 then req_user_id = 11; ** tim sheehy;
		else if bsi_req_user_id = 608 then req_user_id = 12; ** wen shao;
		else if bsi_req_user_id in (208,288,1165,1277,1325,1361) then req_user_id = 20; ** other IMS user;
		else if bsi_req_user_id in (988,994,1283,1348,1479,1528) then req_user_id = 21; ** DESL user;
		else if bsi_req_user_id in (58,524,638,935,936,1570,1627,35000495,35000361) then req_user_id = 22; ** BPTL user;
		else if bsi_req_user_id in (237,292,771,789) then req_user_id = 23; ** westat user;
		else if bsi_req_user_id in (13,95,101,253,310,333,337,602,877,912,914,1220,1221,1309,1314,1406,1430,1438,1440,1455,1572,1582,1584,1619,1631,1734,35000342,35000491) 
		 then req_user_id = 24; ** nci at frederick user;
		else if bsi_req_user_id in (76,170,323,589,590,929,954,990,1097,1098) then req_user_id = 25; ** bioreliance user;
		else if bsi_req_user_id in (538) then req_user_id = 26; ** precision user;
		else if bsi_req_user_id in (85,172,534,1271,1308,1387) then req_user_id = 27; ** thermo-fisher user;
		else if bsi_req_user_id in (1319) then req_user_id = 28; ** ppd user;
		else if bsi_req_user_id in (1747,1668) then req_user_id = 29; ** atcc user;
		else if bsi_req_user_id in (1460,35000469) then req_user_id = 40; ** other user;
		else req_user_id = bsi_req_user_id; *** not formatted;
	run;
	
	
	/*
	data all_reqs child_reqs;
		set all_reqs;
		if req_created_child = 1 then output child_reqs;
		else output all_reqs;
	run;*/
	
	
	proc sort data=reqin; by bsi_id requisition_id;
	data child_reqs (keep=bsi_id req_id_created);
		set reqin;
		by bsi_id requisition_id;
		
		length req_id_created $ 12;
		retain req_id_created;
		if first.bsi_id then req_id_created = '';
		
		if not missing(rv_parent) then req_id_created = requisition_id;		
		
		if last.bsi_id then output;
	run;
	
	
	proc sort data=all_reqs tagsort; by bsi_id descending req_dt_mod descending req_dt_submitted requisition_id;
	data lab_reqs;
		set all_reqs;
		by bsi_id descending req_dt_mod descending req_dt_submitted requisition_id;
		if first.bsi_id;
	run; 
	
		  
****************************************************************;
****************************************************************;
****************************************************************;
*********** Requisitions with High Parent Volumes **************;
****************************************************************;
****************************************************************;
****************************************************************;
****************************************************************;
	/*08/07/18 - A large number of vials have been found to selected in requisitions and have children in BSI. They also have full volume. It would be impossible for volume and children to both exist. 
		A small number of vials were pulled by ATCC on a pilot study basis to determine if there were any patterns to the problematic vials, but from the sample no general conclusion could be found. 
		At this point in time, the BSMM has decided against having ATCC investigate the full list, instead IMS will identify and restrict problem requistions and vial. 
		4 requisitions were identified where the parent vials have children in BSI and also have a full volume in the parent volume. These vials will have their own vial status so they can't be selected.
	*/
		
	proc sort data=reqin tagsort; by bsi_id;
	data req_parent_volume;
		set reqin;
		by bsi_id;
		if requisition_id in ('R2011:000185','R2011:001175','R2012:000703','R2013:000645');
		
		serum_high_volume = 1;
	run;
	
	data req_parent_volume;
		set req_parent_volume;
		by bsi_id;
		if first.bsi_id;
	run;
	
	
	data serum_parent;
		set all_vials;
		by bsi_id;
		if mattype = 'B1' and seq_num in ('0003','0004','0009','0010','0028','0029','0038','0039');
	run;
	
	data child_serum;
		set all_vials;
		by bsi_id;
		if mattype = 'B1' and (not missing(source_id) or not missing(parent));
		
		length serum_source $ 12;
		
		if not missing(source_id) then serum_source = source_id;
		else if not missing(parent) then serum_source = parent;
	run;
	
	proc sort data=child_serum tagsort; by serum_source;
	
	data child_serum; 
		set child_serum;
		by serum_source;
		if first.serum_source;
	run;
	
	data serum_parent_volume;
		merge req_parent_volume (in=inprobreq keep=bsi_id)
					child_serum (in=inchildserum keep=serum_source rename=(serum_source = bsi_id));
		by bsi_id;
		
		serum_problem_requisition = inprobreq;
		serum_problem_child = inchildserum;
		if inprobreq and inchildserum then serum_volume_problem = 1;
		else serum_volume_problem = 0;
	run;
	
	
		
****************************************************************;
****************************************************************;
****************************************************************;
******************** GSA Child DNA Vials ***********************;
****************************************************************;
****************************************************************;
****************************************************************;
****************************************************************;
	
	proc sort data=reqin tagsort; by rv_parent;
	proc sort data=gsa_labship_vials tagsort; by bsi_id;
		
	data req_gsa_parents;
		merge gsa_labship_vials (keep=bsi_id in_gsa_lab)
					reqin (in=inreq keep=bsi_id rv_parent req_dt_ls_mod requisition_id req_dt_req rename=(bsi_id = child_bsi_id rv_parent= bsi_id));
		by bsi_id;
		
		retain in_req child_seq_num req_enter_after_gsa ;
		
		if first.bsi_id then do;
			in_req = inreq;
			child_seq_num = substr(child_bsi_id,9,4);
			req_enter_after_gsa = 0;
		end;
	
		if req_dt_req > mdy(01,31,2017) then req_enter_after_gsa = 1;
	run;
	
	proc sort data=req_gsa_parents tagsort; by child_bsi_id;
	data req_gsa_parents;
		set req_gsa_parents;
		by child_bsi_id;
		if last.child_bsi_id;
	run;

****************************************************************;
****************************************************************;
****************************************************************;
********************** Child DNA Vials *************************;
****************************************************************;
****************************************************************;
****************************************************************;
****************************************************************;
	proc sort data=all_vials tagsort; by bsi_id;
	data dna_child (keep= source_all dt_enter);
		set all_vials;
		by bsi_id;
		if mattype = 'CB' and (not missing(parent) or not missing(source_id));
		
		if not missing(source_id) then source_all = source_id;
		else if not missing(parent) then source_all = parent;
	run;
	
	proc sort data=dna_child tagsort; by source_all;
	
	data dna_child1 (keep=source_all child_dna_dt_enter);
		set dna_child;
		by source_all;
		
		retain child_dna_dt_enter;
		if first.source_all then child_dna_dt_enter = .N;
		
		if missing(child_dna_dt_enter) or child_dna_dt_enter < dt_enter then child_dna_dt_enter = dt_enter;
		
		if last.source_all then output;
	run;
	
		

****************************************************************;
****************************************************************;
****************************************************************;
******** Inherit source sequence and source material ***********;
******** from parents where child cannot be determined *********;
****************************************************************;
****************************************************************;
****************************************************************;

	data children;
		set all_vials (keep= bsi_id seq_num mattype parent source_id);
		if not missing(source_id) or not missing(parent);
	run;
	
	data child_vial (keep=bsi_id source_parent source_id parent )
			 child_vial_more (keep=bsi_id source_parent source_id parent);
		set children;
		
		if not missing(source_id) then source_parent = source_id;
		else source_parent = parent;
		
		if substr(source_parent,9,4) > "0113" and substr(source_parent,9,4) not in ('0133','0134') then output child_vial_more; *** needs more recursion;
		else output child_vial;
	run;
	
	proc sort data=child_vial_more tagsort; by source_parent;
	proc sort data=child_vial tagsort; by bsi_id;
		
	data child_vial2 (keep=child_bsi_id source_parent source_id parent);
		merge child_vial_more (in=inmore rename=(bsi_id = child_bsi_id source_parent=bsi_id))
					child_vial;
		by bsi_id;
		if inmore;
	run;	
	
	proc sort data=child_vial tagsort; by bsi_id;
	proc sort data=child_vial2 tagsort; by child_bsi_id;
	
	data all_child_vials;
		merge child_vial (rename=(bsi_id= child_bsi_id))
					child_vial2;
		by child_bsi_id;
	run;
	
	proc sort data=all_child_vials tagsort; by source_parent;
	data child_w_source1 (keep=child_bsi_id source_mat source_seq)
			 child_w_source_more (keep=child_bsi_id source_parent2 source_id parent);
		merge all_child_vials (in=inchild keep= child_bsi_id source_parent source_id parent rename=(source_parent = bsi_id))
					all_vials (keep= bsi_id mattype seq_num);
		by bsi_id;
		
		length source_mat $ 3 source_seq $ 4;
		if inchild then do;
			if mattype ^= 'CB' then do;
				source_mat = mattype;
				source_seq = seq_num;
			end;
			
			if not missing(source_seq) then output child_w_source1;
			else do;
				if not missing(source_id) then source_parent2 = source_id;
				else source_parent2 = parent;
				output child_w_source_more;
			end;
		end;
	run;
	
	
	proc sort data=child_w_source_more tagsort; by source_parent2;
	proc sort data=child_w_source1 tagsort; by child_bsi_id;
		
	data child_w_source2 (keep=child_bsi_id source_mat source_seq)
			 child_w_source_more2 (keep=child_bsi_id source_parent2 source_id parent);
		merge child_w_source_more (in=inchild keep= child_bsi_id source_parent2 source_id parent rename=(source_parent2 = bsi_id))
					child_w_source1 (keep= child_bsi_id source_mat source_seq rename=(child_bsi_id = bsi_id));
		by bsi_id;
		if inchild;
			
		if not missing(source_seq) then output child_w_source2;
		else do;
			if not missing(source_id) then source_parent2 = source_id;
			else source_parent2 = parent;
			output child_w_source_more2;
		end;		
	run;	
	
	proc sort data=child_w_source_more tagsort; by source_parent2;
	proc sort data=child_w_source2 tagsort; by child_bsi_id;
		
	data child_w_source3 (keep=child_bsi_id source_mat source_seq)
			 child_w_source_more3 (keep=child_bsi_id source_parent3 source_id parent);
		merge child_w_source_more2 (in=inchild keep= child_bsi_id source_parent2 source_id parent rename=(source_parent2 = bsi_id))
					child_w_source1 (keep= child_bsi_id source_mat source_seq rename=(child_bsi_id = bsi_id));
		by bsi_id;
		if inchild;
			
		if not missing(source_seq) then output child_w_source3;
		else do;
			if not missing(source_id) then source_parent3 = source_id;
			else source_parent3 = parent;
			output child_w_source_more3;
		end;		
	run;	
	
	proc sort data=child_w_source_more3 tagsort; by source_parent3;
	data child_w_source4 (keep=child_bsi_id source_mat source_seq);
		merge child_w_source_more3 (in=inchild keep= child_bsi_id source_parent3 rename=(source_parent3 = bsi_id))
					all_vials (keep= bsi_id mattype seq_num);
		by bsi_id;
		if inchild;
		
		length source_mat $ 2 source_seq $ 4;
		
		source_mat = mattype;
		source_seq = seq_num;
	run;	
		
	
	proc sort data=child_w_source1 tagsort; by child_bsi_id;
	proc sort data=child_w_source2 tagsort; by child_bsi_id;
	proc sort data=child_w_source3 tagsort; by child_bsi_id;
	proc sort data=child_w_source4 tagsort; by child_bsi_id;
	data child_w_source (keep=child_bsi_id source_mat source_seq);
		merge child_w_source1
					child_w_source2
					child_w_source3
					child_w_source4;
		by child_bsi_id;
	run;
		
	
****************************************************************;
****************************************************************;
****************************************************************;
******** Inherit Date Recieved for Buccal Cell Vials  **********;
****** Where Date Received Reflects Child and Not Source *******;
****************************************************************;
****************************************************************;
****************************************************************;

	data child_for_buccal;
		set all_vials;
		
		if substr(parent,9,4) in ('0024','0025','0026') or substr(source_id,9,4) in ('0024','0025','0026') then output child_for_buccal;
	run;
	
	proc sort data=child_for_buccal; by source_id;
	proc sort data=all_vials; by bsi_id;
	
	data child_for_buccal;
		merge child_for_buccal (in=inchild keep=source_id bsi_id dt_draw rename=(dt_draw = child_bsi_dt_draw))
					all_vials (keep=bsi_id dt_draw rename=(bsi_id = source_id dt_draw = source_bsi_dt_draw));
		by source_id;
		if inchild;
	run;
	
		
	
****************************************************************;
****************************************************************;
****************************************************************;
********************** Create Vial File ************************;
****************************************************************;
****************************************************************;
****************************************************************;
****************************************************************;
	
	proc sort data=all_vials tagsort; by bsi_id;
	proc sort data=labship_vials tagsort; by bsi_id;	
	proc sort data=sampleid tagsort; by bsi_id;
	proc sort data=bsi_vials tagsort; by bsi_id;
	proc sort data=dna_vials tagsort; by bsi_id;
	proc sort data=lab_reqs tagsort; by bsi_id;
	proc sort data=dna_child1 tagsort; by source_all;
	proc sort data=child_w_source tagsort; by child_bsi_id;
	proc sort data=req_gsa_parents tagsort; by child_bsi_id;
	proc sort data=serum_parent_volume tagsort; by bsi_id;
	proc sort data=bsialiq tagsort; by bsi_id;
	proc sort data=child_reqs tagsort; by bsi_id;	
	proc sort data=child_for_buccal; by bsi_id;
		
	data vial  (keep=	/*IDs*/								BSI_id iid sampleid seq_num pid BCF_sampleid BSI_sampleid old_iid ucla_sampleid subject plco_id
										/*population*/				brooklyn in_ucla in_BCF in_BSI in_MSC MSC special_pop multiple_visit_flag
										/*date and time*/			bd24hour bdampm bdmin bdtime days_collect draw_to_freeze dt_collect bsi_dt_draw dt_enter dt_mod dt_proc dt_rcvd samp_dt_mod shipdate bdhour days_rcvd bsi_days_draw
										/*bcf info*/					rndgroup study_yr studyyr
										/*bsi info*/					label tests thaws remp BSI_vialstat mattype reposid studyid vial_type new_vial_type mod1-mod10 value1-value10 warning1-warning10 relabel verified_status parent source_id shipment_vial_id scode1-scode16 scomm1-scomm16 
																					vcode1-vcode15 vcomm1-vcomm15 aliquot_use1-aliquot_use10 vault
										/*DNA info*/					DNA_conc DNA_extract_mthd DNA_mass_ug DNA_quant_mthd DNA_status iden_DNA source_DNA staged_DNA DNA_concentration DNA_mass DNA_mass_unit DNA_usability dna_stat dna_cat a260_280 bptl_DNA_quant_dt
																					DNAS_avail_stat DNAS_avail_est DNAS_avail_cgrpre DNAS_avail_noncgr DNAS_avail_accept
										/*volume*/						full vol_est volume_mL vol_unit volume
										/*vial info*/					plastic thawed arrived_thawed buccal is_parent source_mat source_seq tube_type vialstat ytop buccal_fraction plastic_tube robot_popoff_tube buffy_fraction vial_created_by
										/*labship*/						EEMS_all EEMS_no_count EEMS_no1-EEMS_no12 labdate1-labdate12 gsa_source gsa_dna 
										/*requisition info*/	req_id req_status req_dt_mod req_dt_submitted req_investigators req_status req_vial_status req_user_id req_reason req_vial_reason req_id_created req_last_reposid
										/*location*/					freezer freezer_location_id	box rack row col
										/*2017 buccal*/				buccal_cdcc buccal_cdcc_cond1-buccal_cdcc_cond4 buccal_cdcc_num_cond buccal_cdcc_fraction
										/*temporary*/ 				desl_sampleid_usable desl_sampleid_unusable);
		length ucla_sampleid bcf_sampleid $ 8;
		merge all_vials (in=invial rename=(vialstat = bsi_vialstat bcf_sampleid = old_bcf_sampleid dt_draw = bsi_dt_draw sampleid = long_sampleid))
					labship_vials (in=inlabship keep= bsi_id EEMS_no_count eems_no1-eems_no12 labdate1-labdate12 dna_extract1-dna_extract12 has_labship lien1-lien12 analyte_category1-analyte_category12)
					sampleid (keep= bsi_id bcf_sampleid iid old_iid dt_draw)
					bsi_vials (keep= bsi_id vstat lab_after dna_rep in_desl dna_mod)
					dna_vials (in=indna keep= bsi_id avail_dna dna_mod_all in_desl rename=(in_desl=dna_in_desl))
					lab_reqs (in=inreq keep=requisition_id bsi_id req_dt_mod req_dt_submitted req_investigators req_status req_vial_status req_user_id req_reason req_vial_reason req_last_reposid rename=(requisition_id= req_id))
					dna_child1 (in=inchilddna keep=source_all child_dna_dt_enter rename=(source_all = bsi_id))
					child_w_source (in=inchildsource keep= child_bsi_id source_mat source_seq rename=(child_bsi_id = bsi_id))
					req_gsa_parents (in=inreqgsa keep= child_bsi_id req_enter_after_gsa in_gsa_lab rename=(child_bsi_id = bsi_id))
					serum_parent_volume (in=inserum keep=bsi_id serum_volume_problem)
					bsialiq (in=inaliq keep=bsi_id aliquot_use1-aliquot_use10)
					child_reqs (in=inchildreq keep=bsi_id req_id_created)
					child_for_buccal (in=inchildbuccal keep=bsi_id child_bsi_dt_draw source_bsi_dt_draw);
		by bsi_id; 
		if invial;
				
		length eems_all $ 85 vialstat $ 2 sampleid $ 8 ;
		
		
		**** array set up;
		scode_num = 16;
		mod_num = 10;
		value_num = 10;
		vcode_num = 15;
		labship_num = 12;
		warning_num = 10;
		aliquot_num = 5;
		
		array a_scode[1:16] scode1-scode16;
		array a_scomm[1:16] scomm1-scomm16;
		array a_mod[1:10] mod1-mod10;
		array a_value[1:10] value1-value10;
		array a_vcode[1:15] vcode1-vcode15;	
		array a_vcomm[1:15] vcomm1-vcomm15;	
		array a_EEMS_no[1:12] EEMS_no1-EEMS_no12;
		array a_labdate[1:12] labdate1-labdate12;
		array a_warning[1:10] warning1-warning10;
		array a_aliquot_use[1:10] aliquot_use1-aliquot_use10;
		
		
		**** set IDs;
		if missing(pid) and not missing(vial_pid) then pid = vial_pid;
		if missing(pid) and not missing(ucla_pid) then pid = ucla_pid;
		
		if missing(pid) or missing(subject) or pid = '-' then do;
			if (missing(pid) or pid = '-') and missing(subject) then has_miss_id = 1;
			else if missing(pid) or pid = '-' then pid = cat(substr(subject,1,6),'-',substr(subject,7,1));
**			else if missing(subject) then subject = cat(substr(pid,1,6),substr(pid,8,1));
		end;
		
		
		sampleid = long_sampleid;
		if sampleid ne substr(bsi_id,1,7) then sampleid = substr(bsi_id,1,7);
		
		bsi_sampleid = sampleid;
		if missing(bcf_sampleid) then bcf_sampleid = substr(bsi_id,1,7);
		if substr(bsi_id,1,2) in ('UA','UB','UC','UD','UE') then do i = 1 to 15;
			if a_vcode[i] = 'SAMPLEID' then bcf_sampleid = cat(substr(a_vcomm[i],1,2),' ',substr(a_vcomm[i],3,4));
		end;						
		if index(bcf_sampleid,'if 7795') then bcf_sampleid = 'IF 7795';

		
		
		*** set missing codes;
		if missing(dt_proc) then dt_proc = .M;
		if missing(dt_rcvd) then dt_rcvd = .M;
		if missing(shipment_vial_id) then shipment_vial_id = .M;
		if missing(vial_type) then vial_type = .M;
		if missing(new_vial_type) then new_vial_type = .M;
		if missing(dna_concentration) then do;
			if mattype in ("CB") then dna_concentration = .M;
			else dna_concentration = .N;
		end;
		if missing(dna_mass) then do;
			if mattype in ("CB") then dna_mass = .M;
			else dna_mass = .N;
		end;
		if missing(a260_280) then do;
			if mattype = 'CB' then a260_280 = .M;
			else a260_280 = .N;
		end;
		if missing(verified_status) then verified_status = 0;
		if missing(relabel) then relabel = 0;
**		if missing(in_ucla) then in_ucla = 0;
**		if missing(bsi_dt_draw) and mattype in ('CH') then bsi_dt_draw = .N;
		if missing(bsi_dt_draw) then bsi_dt_draw = .M;
		
		if missing(bdhour) and mattype in ("CH") then bdhour = .N;
		else if missing(bdhour) then bdhour = .M;
		if missing(bdmin) and mattype in ("CH") then bdmin = .N;
		else if missing(bdmin) then bdmin = .M;
		if missing(bdampm) and mattype in ("CH") then bdampm = .N;
		else if missing(bdampm) then bdampm = .M;
		if dna_mass_ug = . and mattype ne "CB" then dna_mass_ug = .N;
		else if dna_mass_ug = . and mattype = "CB" then dna_mass_ug = .M;
	 	if missing(freezer_location_id) then freezer_location_id = .M;
	 	if missing(dna_usability) and mattype ne 'CB' then dna_usability = .N;
	 	else if missing(dna_usability) and mattype = 'CB' then dna_usability = .M;
	 	do i = 1 to value_num;
	 		if missing(a_value[i]) then a_value[i] = .N;
	 	end;
		
		*** population variables;
		/*if brooklyn = 1 then in_ucla = 0;
		else*/ if mattype = 'B1' and seq_num in ('0001','0002') then in_ucla = 1;
		else if mattype = 'B1' and (substr(source_id,9,4) in ('0001','0002') or substr(parent,9,4) in ('0001','0002')) then do; 
			in_ucla = 1; 
			ucla_aliquot = 1; 
		end;
		else in_ucla = 0;
		
		in_bsi = 1;
		
		in_bcf = in_sample;
		if vial_in_bcf = 1 then in_bcf = 1;
		
			
		*** buccal 2017 collection;
		if mattype in ("CH","BCR","BSU","CB") and (seq_num in ("0050","0051","0052","0053","0054","0000") or source_seq in ("0050","0051","0052","0053","0054","0000")) and dt_enter >= mdy(11,01,2017) then buccal_cdcc = 1;
		else buccal_cdcc = 0;
			
		*** date drawn for child buccal vials when child buccal vials has different date from the source;
		if inchildbuccal then do;
			if (substr(parent,9,4) in ('0024','0025','0026') or substr(source_id,9,4) in ('0024','0025','0026')) and mattype = 'CB' and 
			 not missing(source_bsi_dt_draw) and bsi_dt_draw ^= source_bsi_dt_draw then bsi_dt_draw = source_bsi_dt_draw;
		end;
		
		*** date collected;
		dt_collect = .M;
		
		if buccal_cdcc = 1 and not missing(bsi_dt_draw) then dt_collect = bsi_dt_draw;
		else if buccal_cdcc = 1 and not missing(dt_rcvd) then dt_collect = dt_rcvd;
	  else if mattype not in ("CH") and not missing(compdate) then dt_collect = compdate;
	  else if mattype not in ("CH") and not missing(vial_compdate) then dt_collect = vial_compdate;
	  else if mattype not in ("CH") and not missing(bsi_dt_draw) then dt_collect = bsi_dt_draw;
		else if mattype = "CH" and not missing(bsi_dt_draw) then dt_collect = bsi_dt_draw;
		else if mattype = "CH" and not missing(dt_rcvd) then dt_collect = dt_rcvd;
		
		*** determine study year;	
		if brooklyn = 1 and '0005' <= seq_num <= '0011' and studyyr = '  ' then studyyr= '00'; **** brooklyn hard codes;
		else if brooklyn = 1 and '0014' <= seq_num <= '0023' and studyyr = '  ' then studyyr= '03'; **** brooklyn hard codes;
		else if not missing(brooklyn_studyyr) then studyyr = brooklyn_studyyr;
		
		if not missing(studyyr) then study_yr = input(studyyr,2.);
		else if not missing(vial_studyyr) then study_yr = input(vial_studyyr,2.);
		else if mattype='CH' or substr(right(source_id),11,4) = '0024' or brooklyn = 1 or buccal_cdcc = 1 then do;
			if rnddate > .Z and dt_collect > .Z then study_yr=YEAR(dt_collect) - YEAR(rnddate) - (MONTH(dt_collect)<MONTH(rnddate)) - (MONTH(dt_collect)=MONTH(rnddate) AND DAY(dt_collect)<DAY(rnddate));
			else if rnddate > .Z and dt_rcvd > .Z then study_yr=YEAR(dt_rcvd) - YEAR(rnddate) - (MONTH(dt_rcvd)<MONTH(rnddate)) - (MONTH(dt_rcvd)=MONTH(rnddate) AND DAY(dt_rcvd)<DAY(rnddate));
		end;	
				
		if study_yr < 0 or missing(study_yr) then study_yr = .M;	
		if not missing(study_yr) and missing(studyyr) and study_yr <= 9 then studyyr = cat('0',put(study_yr,1.));
		if not missing(study_yr) and missing(studyyr) and study_yr >= 10 then studyyr = put(study_yr,2.);	
	
		
		
		**** correct mattype;
		do i = 1 to warning_num;
			if a_warning[i] in ("QT") then warning_mattype = 1;
		end;
		if mattype = '99' and in_ucla = 1 and warning_mattype ne 1 then mattype = 'B1';
		if not missing(DNA_CONCENTRATION) and mattype not in ("CB") then mattype = "CB"; 
									
		
		**** labship;
		if not inlabship then do;
			eems_all = '';
			labship_when = 5;
			EEMS_no_count = .N;
			do i = 1 to labship_num;
				a_EEMS_no[i] = '';
				a_labdate[i] = .M;
			end;
		end;
		


		gsa_source = 0;
		do i = 1 to labship_num;
			if ((esc1 = 1 and esc2 = 1) or seq_num in ('0050','0051','00052','0053','0054')) and (index(a_EEMS_no[i],'Global Screen') or index(upcase(a_EEMS_no[i]),'GSA')) then gsa_source = 1;
			else if not((esc1 = 1 and esc2 = 1) or seq_num in ('0050','0051','00052','0053','0054')) and (index(a_EEMS_no[i],'Global Screen') or index(upcase(a_EEMS_no[i]),'GSA')) then do;
				a_EEMS_no[i] = '';
				a_labdate[i] = .M;
			end;
		end;	
	
		do i = 1 to labship_num;
			if not missing(a_EEMS_no[i]) then do;
				if missing(eems_all) then eems_all = a_EEMS_no[i];
				else eems_all = cats(eems_all,'/',a_EEMS_no[i]);
			end;
		end;
		
		
		**** Set vial status;
		do i = 1 to warning_num;
			if a_warning[i] in ("B","VB") then warning_broken = 1;
			if a_warning[i] in ("A") then warning_empty = 1;
		end;		
		do i = 1 to vcode_num;
			if a_vcode[i] in ("RESERVED") and a_vcomm[i] in ("1") then vcode_reserved = 1;
			if a_vcode[i] in ("USE") and a_vcomm[i] in ("2") then vcode_reserved = 1;
		end;
		do i = 1 to mod_num;
			if a_mod[i] in ("YN") then mod_broken = 1;
		end;
		
		*** temporary fix for vials going to Tim Church's lab - not available for additional selections;
		do i = 1 to labship_num;
			if a_EEMS_no[i] = '2006-0060' then tim_church = 1;
		end;
		if tim_church = 1 and bsi_vialstat = '3' then vialstat='2';
		
		do i = 1 to vcode_num;
			if a_vcode[i] = 'HALF' and mattype = 'CC' and a_vcomm[i] in (' ','1') then buffy_usedhalf=1; 
		end;
					
	  do i = 1 to 12;
	  	if a_EEMS_no[i] = '2009-0516' and a_labdate[i] = mdy(08,30,2010) then bad_plasma = 1;
	  end;
  		
  		
		if dna_usability in (7,10) or in_ovar = 1 /*or reposid = 'S' or bad_plasma = 1*/ then vialstat = 'X';
		else if sampleid in ('IK 3035') then vialstat = 'M';
		else if warning_broken = 1 then vialstat = '5';
		else if pid in ('517486-0','806088-4') and study_yr =5 and '0100' <= seq_num <= '0112' then vialstat='9'; 
		else if bsi_vialstat in ('1','3','7') and dna_mass_ug = 0 and mattype = 'CB' then vialstat = '6';
		else if buccal_cdcc = 1 then do; *** CDCC buccal collection;
			if index(eems_all,'Global Screen Array') and volume_ml > 0 and reposid = 'E' and not missing(req_id) and eems_no1 = 'Global Screen Array' and bsi_vialstat in ('1','3','7') then vialstat = bsi_vialstat; 
			else if not missing(eems_no1) and ((bsi_vialstat in ('3','7')) or (bsi_vialstat = '1' and labdate1 > req_dt_mod)) then vialstat = 'E';
			else vialstat = bsi_vialstat; 
		end; 
		else if mattype in ('CC','CS','CH','B4') then do; *** DNA source;
			if mattype = 'CH' and substr(bsi_sampleid,1,2) = 'UI' then vialstat='P';
			else if 0 <= volume_ml < .05 then vialstat = '6';
			else if bsi_vialstat not in ('2','4','5','6') and .5 <= volume_ml < 1.8 and req_status = 1 and req_dt_mod > labdate1 and reposid = 'E' then vialstat = bsi_vialstat;
			else if bsi_vialstat = '6' then vialstat = '6';
			else if seq_num ne '0024' and (seq_num <='0113' or (seq_num in ('0133','0134') and mattype = 'CC')) then do;
				if index(eems_all,'Global Screen Array') and volume_ml > 0 and reposid = 'E' and not missing(req_id) and eems_no1 = 'Global Screen Array' and bsi_vialstat in ('1','3','7') then vialstat = bsi_vialstat; 
					**** partial extractions were done for the GSA and vials should be available;
				else if not missing(eems_all) and vstat in (1,2,3,4) and lab_after = 1 and dna_rep = 1 /*and in_desl = .N*/ then do;
					if upcase(analyte_category1) = 'BIOCHEMICAL' then vialstat = 'S';
					else vialstat = 'E';  *** vial has recent labship and is not at desl - set to pending, were expecting desl to extract;
				end;
				else if not missing(eems_all) and lab_after = 1 and child_dna_dt_enter < labdate1 then do;
					if upcase(analyte_category1) = 'BIOCHEMICAL' then vialstat = 'S';
					else vialstat = 'E'; *** vial has recent labship and no recent DNA;
				end;
				else if not missing(eems_all) and vstat = 1 and lab_after = 0 then vialstat = '6'; *** vial is pending, but no recent labship - set to empty, we dont expect to get any dna;
				else if not missing(eems_all) and vstat = 1 and missing(lab_after) then vialstat = '6';  *** vial is pending, but with no labship - set to empty, we dont expect to get any dna;
				else if not missing(eems_all) and vstat = 1 and lab_after = 1 and dna_mod = 1 and gsa_source = 1 and volume_ml = 0 then vialstat = '6'; *** vial is pending with recent labship and recent dna modification - set to empty, weve already received dna;
				else vialstat = bsi_vialstat;
			end;
			else vialstat = bsi_vialstat;
		end;
		else if mattype = 'CB' then do; *** DNA;
			if not missing(last_date_known_unusable) and reposid = 'F' then do;
				if dt_enter > last_date_known_unusable or req_dt_mod > last_date_known_unusable then do; 
					vialstat = bsi_vialstat;
					desl_sampleid_usable = 1;
				end;
				else do;
					vialstat = '6';
					desl_sampleid_unusable = 1;
				end;
			end;
			if vialstat ^= '6' then do;
				if missing(dna_mass_ug) and bsi_vialstat not in ('4','5','6') and req_dt_mod >= mdy(03,14,2014) and dt_enter >= mdy(03,14,2014) then vialstat = 'E';
				else if not missing(volume_mL) and bsi_vialstat not in ('4','5','6') and (req_dt_mod >= mdy(03,14,2014) or dt_enter >= mdy(03,14,2014)) then vialstat = bsi_vialstat; 
				else if missing(dna_mass_ug) and bsi_vialstat in ('1','3','7') and req_dt_mod < mdy(03,14,2014) then vialstat = '6';
				else if reposid = 'F' and avail_dna = 1 and dna_mod_all = 0 and dna_in_desl = 0 and bsi_vialstat ne '5' then vialstat = '6';  
				else vialstat = bsi_vialstat;
			end;
		end;
		else if mattype in ("B1") and bsi_vialstat = '1' and reposid = 'E' and volume_ml > 1.8 and serum_volume_problem = 1 then vialstat = 'V'; *** serum or plasma with questionable volume because of children;
		else if mattype in ("B1","B2") then do; *** serum or plasma;
			if bsi_vialstat = '6' or 0 <= volume_ml < .05 then vialstat = '6';
			else if not missing(eems_no1) and ((bsi_vialstat in ('3','7')) or (bsi_vialstat = '1' and labdate1 > req_dt_mod and ((dt_mod <= mdy(01,01,2018) or labdate1 > dt_mod)))) then vialstat = 'S';
			else vialstat = bsi_vialstat;
		end;
		else vialstat = bsi_vialstat; 
		if vialstat in ('1','3','7','S','E') and in_discord then vialstat = 'Y';
		
		
		
	
		**** special population;
		if brooklyn = 1 or in_ucla = 1 or vialstat = 'P' then special_pop = 1;
		else special_pop = 0;
  	
		**** time and dates of blood draw;
		bdtime = .N;
		bd24hour = .N;
		days_collect = .M;
		bsi_days_draw = .N;
		
		if not missing(dt_rcvd) then do;
			if brooklyn = 1 and not missing(brook_rnddate) then days_rcvd = dt_rcvd - brook_rnddate;
			else if not missing(rnddate) then days_rcvd = dt_rcvd - rnddate;
			else days_rcvd = .M;
		end;
		else days_rcvd = dt_rcvd;
		
		if not missing(bsi_dt_draw) then do;
			if brooklyn = 1 and not missing(brook_rnddate) then bsi_days_draw = bsi_dt_draw - brook_rnddate;
			else if not missing(rnddate) then bsi_days_draw = bsi_dt_draw - rnddate;
			else bsi_days_draw = .M;
		end;
		else bsi_days_draw = bsi_dt_draw;
		
		
		if brooklyn = 1 then do;
			if not missing(dt_collect) and not missing(brook_rnddate) then days_collect = dt_collect - brook_rnddate;
			else days_collect = .M;
		end;
		else do;
			if not missing(dt_collect) and not missing(rnddate) then days_collect = dt_collect - rnddate;
			else days_collect = .M;
		end;
		
		if mattype not in ("CH") then do;
			fr24hour = .N;
			bdtime = .N;
			
			if missing(bdhour) and not missing(vial_bdhour) then bdhour = vial_bdhour;
			if missing(bdmin) and not missing(vial_bdmin) then bdmin = vial_bdmin;
			if missing(bdampm) and not missing(vial_bdampm) then bdampm = vial_bdampm;
			
			if missing(frozampm) and not missing(vial_frozampm) then frozampm = vial_frozampm;
			if missing(frozhour) and not missing(vial_frozhour) then frozhour = vial_frozhour;
			if missing(frozmin) and not missing(vial_frozmin) then frozmin = vial_frozmin;
			if missing(frampm) and not missing(vial_frampm) then frampm = vial_frampm;
			if missing(frmin) and not missing(vial_frmin) then frmin = vial_frmin;
			if missing(frhour) and not missing(vial_frhour) then frhour = vial_frhour;
			
			*** fix blood draw errors for hour and minute;
			if bdhour = 0 then bdhour = 10; 
			if not missing(bdhour) and missing(bdmin) then bdmin = 0;
			
			 
			*** normalize blood draw and freeze hours and minutes;
			draw_min = .N;
			hour_min = .N;
			frozen_min = .N;
			frozen_hour = .N;
			frozen_min2 = .N;
			frozen_hour2 = .N;
			
			
			if not missing(bdhour) then draw_hour = bdhour;
			else draw_hour = .M;
			if not missing(bdmin) then draw_min = bdmin;
			else draw_min = 0;
			
			if not missing(frhour) then frozen_hour = frhour;
			else frozen_hour = .M;
			if not missing(frmin) and index(frmin,'*') = 0 then frozen_min = frmin;
			else frozen_min = 0;
			frozen_ampm = frampm;
			
			if not missing(frozhour) then frozen_hour2 = frozhour;
			else frozen_hour2 = .M;
			if not missing(frozmin) and index(frozmin,'*') = 0 then frozen_min2 = input(frozmin,2.);
			else frozen_min2 = 0;
			frozen_ampm2 = input(frozampm,1.);
			
			**** correct am/pm variables;
			if ((missing(bdampm) or bdampm = 2) and draw_hour in (8:11)) then bdampm = 1;
			if (missing(bdampm) or bdampm = 1) and (draw_hour in (1:5) or draw_hour >= 12) then bdampm = 2;
			
			if (missing(frampm) or frampm = 2) and ((bdampm ne 2 and draw_hour > frozen_hour and frozen_hour < 10) or (bdampm ne 2 and draw_hour < frozen_hour and frozen_hour in (10:11))) 
				and frozen_hour in (8:11) then frozen_ampm = 1;
			if (missing(frampm) or frampm = 1) and (bdampm ne 1 and draw_hour < frozen_hour) and (frozen_hour in (1:5) or frozen_hour >= 12) then frozen_ampm = 2;
			if frampm = 1 and bdampm = 2 and frozen_hour ne 11 then frozen_ampm = 2;
			
			if (missing(frozampm) or frozampm = '2') and ((bdampm ne 2 and draw_hour > frozen_hour2 and frozen_hour2 < 10) or (bdampm ne 2 and draw_hour < frozen_hour2 and frozen_hour2 in (10:11))) 
				and frozen_hour2 in (8:11) then frozen_ampm2 = 1;
			if (missing(frozampm) or frozampm = '1') and (bdampm ne 1 and draw_hour < frozen_hour2) and (frozen_hour2 in (1:5) or frozen_hour2 >= 12) then frozen_ampm2 = 2;
			if frozampm = 1 and bdampm = 2 and frozen_hour2 ne 11 then frozen_ampm2 = 2;
			
			*** bd 24 hour;
			if bdampm = 2 and not missing(bdhour) and bdhour in (1:11) then bd24hour = bdhour + 12;
			else if bdampm = 2 and not missing(bdhour) then bd24hour = bdhour;
			else if bdampm = 1 and not missing(bdhour) then bd24hour = bdhour;
			else if missing(bdampm) or missing(bdhour) then bd24hour = .M;
			
			**** calculate draw to freeze;
			frozen24hour = .N;
			frozen24hour2 = .N;
			bdtime = .N;
			
			if not missing(draw_hour) and bdampm = 2 and draw_hour in (1:11) then draw_hour = draw_hour + 12;
			if not missing(frozen_hour) and frozen_ampm = 2 and frozen_hour in (1:11) then frozen_hour = frozen_hour + 12;
			if not missing(frozen_hour2) and frozen_ampm2 = 2 and frozen_hour2 in (1:11) then frozen_hour2 = frozen_hour2 + 12;
			
			if not missing(draw_hour) then bdtime = (draw_hour * 60) + draw_min;
			if not missing(frozen_hour) then frozen24hour = (frozen_hour * 60) + frozen_min;
			if not missing(frozen_hour2) then frozen24hour2 = (frozen_hour2 * 60) + frozen_min2;
			
			if missing(bdtime) or (missing(frozen24hour) and missing(frozen24hour2)) then draw_to_freeze = .M;
			else if seq_num in ('0001','0002') and not missing(frozen24hour2) and frozen24hour2 >= bdtime then draw_to_freeze = frozen24hour2 - bdtime;
			else if seq_num not in ('0001','0002') and not missing(frozen24hour) and frozen24hour >= bdtime then draw_to_freeze = frozen24hour - bdtime;
			else draw_to_freeze = .M;	
		end;
		else draw_to_freeze = .N;
	
		
		if (missing(shipdate)) or (seq_num not in ('0001','0002')) or (seq_num = '0001' and vialnum = '002') or (seq_num = '0002' and vialnum = '001') then shipdate = .M;
		
				
		
		
		
		
		**** source information;
		if missing(source_seq) and not missing(source_id) then source_seq = substr(source_id,9,4);
		if missing(source_mat) or source_mat = 'CB' then do;  
			if '0136' <= seq_num <= '0140' or '0136' <= source_seq <= '0140' then source_mat='B4';
			if '0165' <= seq_num <= '0178' or '0165' <= source_seq <= '0178' then source_mat='CC';
			if '0320' <= seq_num <= '0328' or '0320' <= source_seq <= '0328' then source_mat='CC';
			if '0415' <= seq_num <= '0431' or '0415' <= source_seq <= '0431' then source_mat='B4';
			if '0526' <= seq_num <= '0548' or '0526' <= source_seq <= '0548' then source_mat='CC';
		end;
		
		if mattype = 'CB' and source_mat not in ('CH','B4','CC','CS') then do;	
			if source_seq = '0016' then source_mat = 'CC';
			else if source_seq in ('0024','0025','0026') then source_mat = 'CH';
			else if substr(sampleid,1,2) in ('PL','PC') then source_mat = 'CH';
			else if study_yr = 0 then source_mat = 'CC';
			else if study_yr = 4 then source_mat = 'CS';
			else if sampleid in ('UA 2730','UB 1175') then source_mat = 'B4';
			else if sampleid = 'IY 7490' then do;
				source_id = 'IY 7490 0016';
				source_mat = 'CC';
			end;
			else if sampleid = 'UP 5736' then do;
				source_id = 'UP 5736 0042';
				source_mat = 'CC';
			end;
		end;
		
		*** buccal cells do not have date draw. Child DNA shouldn't either;
		*if source_mat = 'CH' and mattype = 'CB' then bsi_dt_draw = .N;
		
		**** vial types;		
		do i = 1 to 10;
			if a_mod[i] = 'B8' then edta_plasma = 1;
		end;
		
		if seq_num in ("0001","0002") or source_seq in ("0001","0002") then tube_type = 5; *** ucla;
		else if seq_num in ("0003","0004","0009","0010","0028","0029","0038","0039") or source_seq in ("0003","0004","0009","0010","0028","0029","0038","0039") then tube_type = 3; *** red top;
		else if seq_num in ("0005","0006","0007","0008","0014","0015","0016") or source_seq in ("0005","0006","0007","0008","0014","0015","0016") then tube_type = 1; *** green top;
		else if seq_num in ("0011") or source_seq in ("0011") then tube_type = 4; *** royal blue;
		else if (mattype ne 'B1' and seq_num in ("0012")) or seq_num in ("0013","0100","0101","0102","0103","0104","0105","0106","0107","0108","0109","0110","0111",'0112') 
						or source_seq in ("0012","0013","0100","0101","0102","0103","0104","0105","0106","0107","0108","0109","0110","0111",'0112') then tube_type = 6; *** yellow top;
		else if seq_num in ("0017","0018","0019","0020","0021","0023","0030","0031","0040","0041","0042","0043","0044","0045","0046") or source_seq in ("0017","0018","0019","0020","0021","0023","0030","0031","0040","0041",
												"0042","0043","0044","0045","0046") or (mattype = 'B2' and edta_plasma = 1) then tube_type = 2; *** lavender;
		else tube_type = .M;

		if tube_type = .M then do;
		   if study_yr = 0 and mattype = 'B2' then tube_type = 1;
		   if study_yr in (4,5) and mattype = 'B2' then tube_type = 2;
		   if mattype = 'B1' then tube_type = 3;     /* assume red-top if we don't know */
		   if mattype = 'B4' or source_mat = 'B4' then tube_type = 6;
		   if (mattype = 'CC' or source_mat = 'CC') and study_yr = 0  then tube_type = 1;
		   if (mattype = 'CC' or source_mat = 'CC') and study_yr in (4,5) then tube_type = 2;
		end;
		
				
		if seq_num in ('0100','0101','0102','0103','0104','0105','0106','0107','0108','0109','0110','0111','0112') and mattype in ("B4") then ytop = 1;
		else if source_seq in ('0100','0101','0102','0103','0104','0105','0106','0107','0108','0109','0110','0111','0112') then ytop = 1;
		else if substr(bsi_id,1,2) in ('UA','UB','UC','UD','UE','UF') then ytop = 1;
		else ytop = 0;
		
		if (seq_num in ("0001","0002","0003","0004","0005","0006","0007","0008","0009","0010","0011","0014","0015","0016","0017","0018","0019","0020","0021","0023","0024","0028","0029","0030","0031","0038","0039",
									"0040","0041","0042","0043","0044","0045","0046") and missing(source_id) and missing(parent)) 
				or (mattype = 'B4' and seq_num in ('0100','0101','0102','0103','0104','0105','0106','0107','0108','0109','0110','0111','0112')) 
				or (mattype = 'CH' and seq_num in ("0025","0026","0027","0028")) 
				or (mattype = 'CC' and seq_num in ('0133','0134')) 
				or (buccal_cdcc = 1 and mattype in ('CH','BCR','BSU'))
				or (mattype = 'B2' and seq_num in ('0034','0035','0133','1000') and missing(source_id) and missing(parent)) then is_parent = 1;
		else is_parent = 0;
		
		*** assume full volume for unselected parent vials that are missing volume;
		if is_parent = 1 and missing(eems_all) and volume_mL = .M and vialstat in ('1') then volume_ml = 1.8;
		
		plastic = 0;
		do i = 1 to vcode_num;
			if index(a_vcode[i],"PLASTIC") then plastic = 1;
		end;
	
		if vial_type = 251 or new_vial_type = 284 then remp = 1;
		else remp = 0;
		
		if index(upcase(freezer),'VAULT') then vault = 1;
		else vault = 0;
		
		if not missing(MSC) or ((new_vial_type in (204,274) or vial_type in (110,111)) and remp ^= 1) then in_MSC = 1;
		else in_MSC = 0;
	
	 		
		**** dna;
**		staged_dna = .N;
		iden_dna = .N;
		dna_status = .N;
		dna_use = .N;
		
		if mattype in ("CB") then do;
**			staged_dna = .M;
			iden_dna = .M;
			dna_status = .M;
			dna_use = .M;
		end;		

		
		if mattype in ("CB") then do;
			do i = 1 to vcode_num;
				if a_vcode[i] = 'USE' then dna_use = input(a_vcomm[i],2.);
**				if a_vcode[i] in ("STAGED") and a_vcomm[i] = '1' then staged_dna = 1;
				if a_vcode[i] in ("IDEN") and a_vcomm[i] = '1' then iden_dna = 1;
				if a_vcode[i] in ("IDEN") and a_vcomm[i] = '0' then iden_dna = 0;
				if a_vcode[i] = 'STAGED' then vial_staged = 1;
				if a_vcode[i] = 'SOURCE' then vial_source = 1;
			end;
		end;
   
**		if dna_usability in (1,12) then staged_dna = 1;
**		if staged_dna =1 then source_dna=1;
**		if dna_usability = 2 then source_dna = 0;
**		if dna_usability in (3,8,14) then source_dna = 1;
		if dna_usability = 5 then iden_dna = 1;
			
		if dna_use = 1 and dna_state = 4 then iden_dna = 1;
		
		
		
		**** dna status variables;
		reserved_dna = .N;
		do i = 1 to vcode_num;
			if a_vcode[i] = 'RESERVED' then reserved_dna = 1;
		end;
		do i = 1 to warning_num;
			if a_warning[i] = 'OI' then outside_inventory = 1;
		end;
   	
		if mattype in ('CB','B4','CC','CH','CS') then do;
			if outside_inventory = 1 then dna_status = 14; 
			else if reserved_dna=1 and vialstat in ('1','3','7','E','S','2','10') then dna_status=2; 
			else if staged_dna=1 and vialstat in ('1','3','7','E','S','2','10') then dna_status=3;
			else if source_dna=1 and vialstat in ('1','3','7','E','S','2','10') then do;
				if (dna_quant_mthd not in ('3','5','7')  and not (dna_usability in (8,14) and dna_quant_mthd not in ('1','4'))) then dna_status=4; 
				else dna_status=5; 
			end;
			else if source_dna ne 1 and mattype = 'CB' and vialstat = '3' and volume_ml = .M then do;
				if dna_mass=.1 and dna_conc <0 then dna_status=11;  /* identifiler */ 
				else if dna_mass >0 and dna_conc <0 then dna_status=12;	 /* reserved for lab study */ 
				else if dna_mass <0  and dna_conc < 0 then dna_status=13; /* virtual */ 
			end;
			else if mattype = 'CB'  and vialstat in ('1','3','7','E','S','2','10') and staged_dna = 0 then dna_status=10; 
			else if dna_state = 0 then dna_status = 8;
			else if vialstat='E' then dna_status=7;
			else if source_dna <0 and mattype = 'CB' and (vialstat = 'E' or dna_incomplete=1)  then dna_status=6; 
			else if dna_use = 1  and vialstat in ('1','3','7','E','S','2','10') then dna_status=11; 
			else if dna_state = 4  and vialstat in ('1','3','7','E','S','2','10') then dna_status=10; 
			else if dna_state = 1  and vialstat in ('1','3','7','E','S','2','10') then dna_status=13; 
			else if missing(source_dna) and mattype = 'CB' and vialstat in ('1','3') then dna_status=8; 
			else if mattype in ('CC','CS','B4','CH') and vialstat in ('1','3') then dna_status=9; 
			if vialstat='E' and mattype='CB' and reposid='F' and dna_status = 7 then dna_status=6 ; /* pending q*/ 
		end;
 		/*
 			1 = ""Distributed""
			2 = ""Reserved""
			3 = ""Staged""
			4 = ""Source""
			5 = "Source, Nanodrop Quantification"
			6 = ""Pending Quantification""
			7 = ""Pending Extraction""
			8 = ""Undocumented""
			9 = ""Raw Source""
			10 = ""Junk""
			11 = ""Identifiler""
			12 = ""Used""
			13 = ""Virtual""
			14 = ""Outside Inventory"""
			*/
 
	 	req_year = substr(req_id,2,4);
		dna_stat = .N;
		dna_cat = .N;
		
		if mattype = 'CB' and vialstat in ('1','3','7','S','E') then do;
			if dna_mass_ug in (.M) then do;
				if vault = 1 and (not missing(dna_quant_mthd) or dt_enter >= mdy(03,14,2014)) then dna_stat = 91;
				else if (vault = 1) or (vault = 0 and (not missing(dna_quant_mthd) or dt_enter >= mdy(03,14,2014))) then dna_stat = 92;
				else dna_stat = 93;
			end;
			else if dna_usability = 9 then dna_stat = 51;
			else if /*vial_staged = 1 or*/ dna_usability in (1,12) then dna_stat = 11;
	 		else if source_dna = 1 then do;
	 			if dna_quant_mthd in ('1','4',' ') then dna_stat = 21;
	 			else if dna_quant_mthd in ('3','5','7') then dna_stat = 22;
	 		end;
	 		else if not missing(a260_280) and not missing(dna_quant_mthd) and not missing(dna_concentration) and not missing(dna_mass_ug) then do;
	 			if dna_quant_mthd in ('1','4',' ') then dna_stat = 23;
	 			else if dna_quant_mthd in ('3','5','7') then dna_stat = 24;
	 		end;
	 		else if not missing(dna_extract_mthd) and not missing(dna_quant_mthd) then do;
	 			if dna_quant_mthd in ('1','4',' ') then dna_stat = 25;
	 			else if dna_quant_mthd in ('3','5','7') then dna_stat = 26;
	 		end;
	 		else if not missing(dna_quant_mthd) then do;
	 			if dna_quant_mthd in ('1','4') then dna_stat = 27;
	 			else if dna_quant_mthd in ('3','5','7') then dna_stat = 28;
	 		end;
	 		else if not missing(dna_extract_mthd) then dna_stat = 81;
	 		else if dna_mass_ug = 5 and dna_concentration = 40 then dna_stat = 82;
	 		else if dna_mass_ug = 5 and dna_concentration = 20 then dna_stat = 83;
	 		else if dna_mass_ug = 5 and dna_concentration = 10 then dna_stat = 84;
	 		else if not missing(dna_mass_ug) and not missing(dna_concentration) then dna_stat = 85;
	 		else if missing(a260_280) and missing(dna_extract_mthd) and missing(dna_quant_mthd) and missing(dna_usability) and missing(dna_concentration) then dna_stat = 86;
	 		else dna_stat = .E;
	 		
			if dna_stat = 11 then dna_cat = 1;
			else if dna_stat in (21,22,23,24,25,26,27,28) then dna_cat = 2;
			else if dna_stat in (51) then dna_cat = 5;
			else if dna_stat in (81,82,83,84,85,86) then dna_cat = 8;
			else if dna_stat in (91,92,93) then dna_cat = 9;
			else dna_cat = .E;
		end;
	 	if dna_cat in (8) then dna_mass_ug = dna_mass_ug/2; *** halve undocumented dna;
 		
 		**** dna for selections;
		if mattype in ('B1','B2','BCR','BSU','C6','F4','M1O','NC','99') or vialstat in ('2','4','5','6','8','9','10','M','P','V','X','Y') then DNAS_avail_stat = .N;
		else if mattype in ('CC','CS','CH','B4') and vialstat in ('1','3','7','S') then DNAS_avail_stat = 1;
		else if mattype in ('CC','CS','CH','B4') and vialstat in ('E') then DNAS_avail_stat = 2;
		else if mattype in ('CC','CS','CH','B4') and in_desl = 1 and vialstat in ('1','3','7','S','E') then DNAS_avail_stat = 3;
		else if mattype in ('CB') and vialstat in ('1','3','7','S','E') then do;
			**** set dna amounts for categories same for all dna categories;
			if source_mat in ('CC','CS','B4') and gsa_dna = 1 then do;	
				dna_select_min = 2.2;
				dna_select_max = 4.2;
			end;
			else if source_mat in ('CC','CS','B4') then do;
				dna_select_min = 1.0;
				dna_select_max = 5.0;
			end;
			else if source_mat in ('CH') and gsa_dna = 1 then do;
				dna_select_min = 3.5;
				dna_select_max = 5.5;
			end;
			else if source_mat in ('CH') then do;
				dna_select_min = 1.0;
				dna_select_max = 5.0;
			end;
				
			if dna_cat = 1 then do;
				if dna_mass_ug < dna_select_min then DNAS_avail_stat = 11;
				else if dna_select_min <= dna_mass_ug < dna_select_max then DNAS_avail_stat = 12;
				else if dna_mass_ug >= dna_select_max then DNAS_avail_stat = 13;
			end;
			else if dna_cat = 2 then do;
				if dna_mass_ug < dna_select_min then DNAS_avail_stat = 21;
				else if dna_select_min <= dna_mass_ug < dna_select_max then DNAS_avail_stat = 22;
				else if dna_mass_ug >= dna_select_max then DNAS_avail_stat = 23;
			end;
			else if dna_cat = 5 then DNAS_avail_stat = 51;
			else if dna_cat = 8 then do;
				if dna_mass_ug < dna_select_min then DNAS_avail_stat = 81;
				else if dna_select_min <= dna_mass_ug < dna_select_max then DNAS_avail_stat = 82;
				else if dna_mass_ug >= dna_select_max then DNAS_avail_stat = 83;
			end;
			else if dna_cat = 9 then DNAS_avail_stat = dna_stat;
		end;
		else DNAS_avail_stat = .E;
		
		
		**** prelim;
		if DNAS_avail_stat in (1,2,12,13,22,23,82,83,91,92) then DNAS_avail_est = 1;
		else if DNAS_avail_stat in (3,11,21,51,81,93) then DNAS_avail_est = 0;
		else DNAS_avail_est = .N;			
		
		**** cgr dna request;
		if reposid = 'F' then do;
			if DNAS_avail_stat in (3,11,12,13,21,22,23,51,81,82,83,91,92,93) then DNAS_avail_cgrpre = 1;
			else if DNAS_avail_stat in (1,2) then DNAS_avail_cgrpre = 0;
			else DNAS_avail_cgrpre = .N;			
		end;
		else DNAS_avail_cgrpre = .N;
		
		**** non cgr dna request;
		if reposid ^= 'F' then do;
			if DNAS_avail_stat in (1,2,12,13,22,23,82,83) then DNAS_avail_noncgr = 1;
			else if DNAS_avail_stat in (3,11,21,51,81) then DNAS_avail_noncgr = 0;
			else DNAS_avail_noncgr = .N;			
		end;
		else DNAS_avail_noncgr = .N;
		
		**** highest level controls;
		if DNAS_avail_stat in (1,2,13,23,83,91) then DNAS_avail_accept = 1;
		else if DNAS_avail_stat in (3,11,12,21,22,51,81,82,92,93) then DNAS_avail_accept = 0;
		else DNAS_avail_accept = .N;			
			
			
		**** full/half vials;
		if mattype = 'B4' and volume_mL >= 1.8 then full = 1;
		else if mattype = 'B1' and volume_mL >= 1.8 then full = 1;
		else if mattype = 'CH' and volume_mL >= 1.5 and vialstat in ('1') then full = 1;
		else if mattype = 'C6' and volume_mL >= 1.8 then full = 1;
		else if mattype = 'B2' and studyyr in ('00','01','02','03','05') and volume_mL >= 1.8 then full = 1;
		else if mattype = 'B2' and studyyr in ('03') and seq_num in ('0017','0018','0019','0020') and volume_mL >= 1 then full = 1;
		else if mattype = 'B2' and studyyr = '04' and volume_mL >= 3.6 then full = 1;
		else if mattype in ('CC','CS') and studyyr in ('00','01','02','03','04','05') and volume_mL >= 1.5 and vialstat = '1' then full = 1;
		else if mattype in ('B2','CC','CS') and missing(studyyr) then full = .M;
		else if mattype in ('B4','B1','CH','C6','B2','CC','CS') then full = 0;
		else full = .M;
		
				   
		**** material problems;
		thawed_warning = 0;
		arrived_thawed = 0;
		do i = 1 to warning_num;
			if a_warning[i] in ('R','T','TF','TN') then thawed_warning = 1;
			if a_warning[i] in ('R','T','TN') then arrived_thawed = 1;
		end;
		
		if thawed_warning = 1 then thawed = 1;
		else if thaws = 0 then thawed = 0;
		else if (seq_num > '0113' or (seq_num in ('0133','0134') and mattype = 'CC')) and thaws in (0,1) then thawed = 0;
		else thawed = 1;
	
		
		robot_popoff_tube = 0;
		if vial_type in (129) or new_vial_type in (45,155,164,174,234,265) then robot_popoff_tube = 1;
		
		plastic_tube = 0;
		if ttype in (8,9) and studyyr = '04' and tdrawn = 1 and seq_num in ('0028','0029') then plastic_tube = 1;
		if ttype in (8,9) and studyyr = '04' and tdrawn = 2 and seq_num in ('0030','0031') then plastic_tube = 1;
		
		if ttype in (8,9) and studyyr = '05' and tdrawn = 1 and seq_num in ('0038','0039') then plastic_tube = 1;
		if ttype in (8,9) and studyyr = '05' and tdrawn = 2 and seq_num in ('0040','0041','0042','0043') then plastic_tube = 1;
		if ttype in (8,9) and studyyr = '05' and tdrawn = 3 and seq_num in ('0044','0045','0046') then plastic_tube = 1;
				
		if in_verb = 1 then plastic_tube = 1;
		 
  
		**** material types;		
		if mattype in ("CH") or substr(source_id,9,4) in ('0024') then buccal = 1;
		else buccal = 0;
		
		if buccal_cdcc ne 1 and vialstat = '1' and mattype in ("CH") and is_parent = 1 then do;
			if 0 < volume_mL < .5 then buccal_fraction = .33;
			else if .5 <= volume_mL < .9 then buccal_fraction = .5;
			else if .9 <= volume_mL < 1.5 then buccal_fraction = .67;
			else if volume_mL >= 1.5 then buccal_fraction = 1;
			else if missing(volume_mL) then buccal_fraction = .M;
		end;
		else buccal_fraction = .N;
		
		
		if mattype in ('CC','CS') and vialstat in ('1'/*,'3','7','E','S'*/) then do;	
			if 0 < volume_ml < .6 then buffy_fraction = .25;
			else if .6 <= volume_ml < 1.1 then buffy_fraction = .5;
			else if 1.1 <= volume_ml < 1.7 then buffy_fraction = .75;
			else if volume_ml >= 1.7 then buffy_fraction = 1;
			else if missing(volume_ml) then buffy_fraction = .M;
			else if volume_ml = 0 then buffy_fraction = 0;
		end;
		else buffy_fraction = .N;
		
		**** 2017 Buccal Collection;
		array a_buccal_cdcc_cond[1:4] buccal_cdcc_cond1-buccal_cdcc_cond4;
		
		buccal_cdcc_num_cond = 0;
		do i = 1 to 4;
			a_buccal_cdcc_cond[i] = .N;
		end;
		
		if buccal_cdcc = 1 then do;
			shift_place = 0;
			do i = 1 to scode_num;
				if a_scode[i] = 'CONDITION' then do;
					buccal_cdcc_num_cond = 1 + countc(a_scomm[i],',');
					place = 1;
					do j = 1 to buccal_cdcc_num_cond;
						if a_scomm[i] = '- 19' then a_buccal_cdcc_cond[j] = input(substr(a_scomm[i],3,2),2.);
						else a_buccal_cdcc_cond[j] = input(substr(a_scomm[i],place,2),2.);
						
						if substr(a_scomm[i],place+2,2) = ', ' then place = place + 4;		
						else place = place + 3;		
					end;
				end;
			end;
		end;
		
		if buccal_cdcc = 1 and vialstat = '1' and mattype in ("CH") and is_parent = 1 then do;
			if 0 < volume_mL < .5 then buccal_cdcc_fraction = .34;
			else if .5 <= volume_mL < .9 then buccal_cdcc_fraction = .5;
			else if .9 <= volume_mL < 1.5 then buccal_cdcc_fraction = .67;
			else if volume_mL >= 1.5 then buccal_cdcc_fraction = 1;
			else if missing(volume_mL) then buccal_cdcc_fraction = .M;
		end;
		else buccal_cdcc_fraction = .N;
		

		**** UCLA;
		if in_ucla = 1 then ucla_sampleid = bcf_sampleid;


		*** requisition data;
		if not inreq then do;
			req_status = .N;
			req_vial_status = .N;
			req_dt_mod = .N;
			req_dt_submitted = .N;
			req_user_id = .N;
			req_reason = .N;
			req_vial_reason = .N;
		end;
		else if inreq then do;
			if reposid = 'M' then do;
				req_status = 1;
				vial_req_status = 1;
			end;
			if missing(req_dt_submitted) then req_dt_submitted = .M;
		end;
		
		
		**** multiple visit flag for one pancreas id;
		if sampleid = 'IK 3035' then multiple_visit_flag = 1;
		else multiple_visit_flag = 0;
				
		
		*** gsa dna;
		do i = 1 to 15;
			if i <= 12 and (a_EEMS_no[i] = 'Global Screen Array' or index(upcase(a_EEMS_no[i]),'GSA')) then flag_gsa_array = 1;
			if i <= 10 and a_aliquot_use[i] = 'GSA' then flag_gsa_aliquot = 1;
			if index(a_vcomm[i],'GSA') then flag_gsa_comment = 1;
		end;
		
		if mattype = 'CB' then do;
			/*if mdy(02,03,2017) <= dt_enter <= mdy(02,03,2019) and dna_extract_mthd = 'K' then gsa_dna = 1;*/ *** older dates; 
			if mdy(02,03,2017) <= dt_enter <= mdy(05,03,2019) and dna_extract_mthd = 'K' then gsa_dna = 1;
			else gsa_dna = 0;
			/*if in_gsa_lab ^= 1 then gsa_dna = 0;
			else if flag_gsa_aliquot = 1 or flag_gsa_comment = 1 then gsa_dna = 1;
			else if req_enter_after_gsa = 1 and dna_extract_mthd = 'K' then gsa_dna = 1;
			else gsa_dna = 0;*/
		end;
		else gsa_dna = .N;
		
		*** DNA Children are not GSA DNA;
		if gsa_dna = 1 and source_mat = 'CB' then gsa_dna = 0;
		
	run;

	

****************************************************************;
****************************************************************;
****************************************************************;
******** Inherit source sequence and source material ***********;
******** from parents where child cannot be determined *********;
****************************************************************;
****************************************************************;
****************************************************************;
	

	data child_vial (keep=bsi_id source_parent);
		set vial (keep= bsi_id seq_num parent source_id mattype);
		by bsi_id;
		if seq_num > "0113" and not (seq_num in ('0133','0134') and mattype = 'CC');
		
		if not missing(source_id) then source_parent = source_id;
		else source_parent = parent;
	run;
	
	proc sort data=child_vial tagsort; by source_parent;
	proc sort data=vial tagsort; by bsi_id;
		
	data parent_vial;
		merge vial (keep= bsi_id dt_collect days_collect bsi_dt_draw bsi_days_draw rename=(bsi_id = source_parent))
					child_vial (in=inchild);
		if inchild;
		by source_parent;
	run;
	
	
	proc sort data=parent_vial tagsort; by bsi_id;
	proc sort data=vial tagsort; by bsi_id;
		
	data vial;
		merge vial parent_vial (keep= bsi_id dt_collect days_collect bsi_dt_draw bsi_days_draw 
														rename=( dt_collect = parent_dt_collect days_collect = parent_days_collect bsi_dt_draw = parent_bsi_dt_draw 
																		bsi_days_draw = parent_bsi_days_draw ));
		by bsi_id;
		
		if missing(dt_collect) and not missing(parent_dt_collect) then dt_collect = parent_dt_collect;
		if missing(days_collect) and not missing(parent_days_collect) then days_collect = parent_days_collect;
		if missing(bsi_dt_draw) and not missing(parent_bsi_dt_draw) then bsi_dt_draw = parent_bsi_dt_draw;
		if missing(bsi_days_draw) and not missing(parent_bsi_days_draw) then bsi_days_draw = parent_bsi_days_draw;
		
		drop parent_dt_collect parent_days_collect parent_bsi_dt_draw parent_bsi_days_draw;
	run;
		
	
	data child_vial2 (keep=bsi_id source_parent bsi_id seq_num );
		set vial (keep= bsi_id seq_num parent source_id mattype);
		by bsi_id;
		if seq_num > "0113" and not (seq_num in ('0133','0134') and mattype = 'CC');
		
		if not missing(source_id) then source_parent = source_id;
		else source_parent = parent;
	run;
	
	proc sort data=child_vial2 tagsort; by source_parent;
	proc sort data=vial tagsort; by bsi_id;
		
	data parent_vial2;
		merge vial (keep= bsi_id seq_num parent source_id dt_collect days_collect mattype rename=(bsi_id = source_parent))
					child_vial (in=inchild);
		if inchild;
		by source_parent;
	run;
	
	data child_vial2b (keep=source_parent bsi_id);
		set parent_vial2 (keep= bsi_id source_parent seq_num parent source_id mattype);
		by source_parent;
		if seq_num > "0113" and not (seq_num in ('0133','0134') and mattype = 'CC');
		
		if not missing(source_id) then source_parent = source_id;
		else source_parent = parent;
	run;
	

	proc sort data=child_vial2b tagsort; by source_parent;
	proc sort data=vial tagsort; by bsi_id;
		
	data parent_vial2b;
		merge vial (keep= bsi_id dt_collect days_collect bsi_dt_draw bsi_days_draw rename=(bsi_id = source_parent ))
					child_vial2b (in=inchild);
		if inchild;
		by source_parent;
		
		if last.source_parent;
	run;
	
	proc sort data=parent_vial2b tagsort; by bsi_id;
	proc sort data=vial tagsort; by bsi_id;
	data vial;
		merge vial parent_vial2b (keep= bsi_id dt_collect days_collect bsi_dt_draw bsi_days_draw
															rename=(dt_collect = parent_dt_collect days_collect = parent_days_collect bsi_dt_draw = parent_bsi_dt_draw bsi_days_draw = parent_bsi_days_draw));
		by bsi_id;
		
		if missing(dt_collect) and not missing(parent_dt_collect) then dt_collect = parent_dt_collect;
		if missing(days_collect) and not missing(parent_days_collect) then days_collect = parent_days_collect;
		if missing(bsi_dt_draw) and not missing(parent_bsi_dt_draw) then bsi_dt_draw = parent_bsi_dt_draw;
		if missing(bsi_days_draw) and not missing(parent_bsi_days_draw) then bsi_days_draw = parent_bsi_days_draw;
		
		drop parent_dt_collect parent_days_collect parent_bsi_dt_draw parent_bsi_days_draw;
	run;
	

		
****************************************************************;
****************************************************************;
****************************************************************;
********************** Parent EEMS ID **************************;
****************************************************************;
****************************************************************;
****************************************************************;
****************************************************************;	
	data child_eems;
		set vial;
		if (not missing(source_id) or not missing(parent)) and is_parent ^= 1  then output child_eems;
	run;
	
	proc sort data=child_eems tagsort; by parent;
	proc sort data=vial tagsort; by bsi_id;
	
	data eems_id_child;
		merge child_eems (in=inchild keep=parent bsi_id dt_enter mattype vialstat reposid source_id )
					vial (in=inparent keep=bsi_id eems_no1-eems_no12 labdate1-labdate12 rename=(bsi_id =parent));
		by parent;
		
		in_child = inchild;
		in_parent = inparent;
		
		length eems_id_parent $ 25;
		
		array a_eems_no[1:12] eems_no1-eems_no12;
		array a_labdate[1:12] labdate1-labdate12;
		
		num_eems = 0;
		do i = 1 to 12;
			if not missing(a_eems_no[i]) then num_eems = num_eems + 1;
		end;
		
		first_eems_dt = .N;
		if inchild then do;
			if num_eems = 0 then eems_id_parent = 'Missing Parent Linkage';
			else if num_eems = 1 then eems_id_parent = eems_no1;
			else do i = 1 to 12;
				if not missing(a_eems_no[i]) then do;
					if dt_enter > a_labdate[i] and (first_eems_dt = .N or first_eems_dt > a_labdate[i]) then do;
						first_eems_dt = a_labdate[i];
						eems_id_parent = a_eems_no[i];
					end;
				end;
			end;
			if missing(eems_id_parent) then eems_id_parent = 'Missing EEMS Linkage';
		end;
		
	run;
		
	
	proc sort data=vial tagsort; by bsi_id;
	proc sort data=eems_id_child tagsort; by bsi_id;
	
	data vial;
		merge vial (in=invial)
					eems_id_child (in=ineems keep=bsi_id eems_id_parent);
		by bsi_id;
		if invial;
		
	run;

****************************************************************;
****************************************************************;
****************************************************************;
*************** Hemolyzed, Icteric, Lipemic ********************;
****************************************************************;
****************************************************************;
****************************************************************;
****************************************************************;	
	
	proc sort data=all_vials tagsort; by bsi_id;
	proc sort data=bsichar tagsort; by bsi_id;
	data vial_problems vial_problems2;
		merge all_vials (keep= bsi_id sampleid seq_num hemolyzd uclacod1-uclacod4 probcod1-probcod4 )
					vial (in=invial keep=bsi_id in_ucla mod1-mod10 warning1-warning10)
					bsichar (in=inchar keep=bsi_id specimen_char1-specimen_char5);
		by bsi_id;
		if invial;
	
		
		**** array set up;
		array a_mod[1:10] mod1-mod10;
		array a_warning[1:10] warning1-warning10;
		array a_spec_char[1:5] specimen_char1-specimen_char5;
		mod_num = 10;
		warning_num = 10;
		spec_num = 5;
		
		bsi_icteric = 0;
		bsi_lipemic = 0;
		bsi_fibrin_clots = 0;
		warning_hemo = 0;
		do i = 1 to mod_num;
			if index(a_mod[i],'C3') then bsi_icteric = 1;
			if index(a_mod[i],'LIP') /*or index(a_mod[i],'C1')*/ then bsi_lipemic = 1;
		end;
		do i = 1 to warning_num;
			if a_warning[i] = 'LI' then bsi_lipemic = 1;
			if a_warning[i] in ('HE') then warning_hemo = 1;
		end;
		do i = 1 to 5;
			if index(upcase(a_spec_char[i]),'LIPEM') or index(upcase(a_spec_char[i]),'CLOUDY') then bsi_lipemic = 1;
			if index(upcase(a_spec_char[i]),'ICTER') then bsi_icteric = 1;
			if index(upcase(a_spec_char[i]),'FIBRIN') or index(upcase(a_spec_char[i]),'CLOT') then bsi_lipemic = 1;
		end;
					
		if hemolyzd in ("M","R","S","X") or (in_ucla = 1 and uclacod1 = 1) or (probcod1 = 1) or (warning_hemo = 1) or (bsi_hemo = 1) then hemo = 1;
		else if hemolyzd in ("C","A","U") then hemo = 9;
		else hemo = 0;
		
		if bsi_icteric = 1 or (uclacod2 = 1 and in_ucla = 1) or (probcod2 = 1) then icteric = 1;
		else icteric = 0;
		
		if bsi_lipemic = 1 or (in_ucla = 1 and uclacod3 = 1) or (probcod3 = 1) then lipemic = 1;
		else lipemic = 0;
		
		
		if hemo in (0) then hemo_source = 0;
		else if hemo = 9 then hemo_source = 1;
		else if in_ucla ne 1 then do;
			if (hemolyzd in ("M","R","S","X","C","A","U") or warning_hemo = 1) and (probcod1 ne 1) then hemo_source = 1;
			else if (hemolyzd not in ("M","R","S","X","C","A","U") and warning_hemo ^= 1) and probcod1 = 1 then hemo_source = 2;
			else if (hemolyzd in ("M","R","S","X","C","A","U") or warning_hemo = 1)  and (probcod1 = 1) then hemo_source = 3;
			else hemo_source = 6;
		end;
		else if in_ucla = 1 then do;
			if (hemolyzd not in ("M","R","S","X","C","A","U") and warning_hemo ^= 1) and (uclacod1 = 1 or probcod1 = 1) then hemo_source = 4;
			else if (hemolyzd in ("M","R","S","X","C","A","U") or warning_hemo = 1) and (uclacod1 = 1 or probcod1 = 1) then hemo_source = 5;
			else if (hemolyzd in ("M","R","S","X","C","A","U") or warning_hemo = 1) then hemo_source = 1;
			else hemo_source = 6;
		end;
	
	
		if hemo = 0 then hemo_grade = .N;
		else if hemo = 1 then do;
			if hemolyzd = "X" then hemo_grade = 1;
			else if hemolyzd = "R" then hemo_grade = 2;
			else if hemolyzd = "M" then hemo_grade = 3;
			else if hemolyzd = "S" then hemo_grade = 4;
			else hemo_grade = 9;
		end;
		else if hemo = 9 then hemo_grade = 10;
		
		if icteric = 0 then icteric_source = 0;
		else if in_ucla ne 1 then do;
			if bsi_icteric = 1 and (probcod2 ne 1) then icteric_source = 1;
			else if (probcod2 = 1) and bsi_icteric ne 1 then icteric_source = 2;
			else if probcod2 = 1 and bsi_icteric = 1 then icteric_source = 3;
			else icteric_source = 6;
		end;
		else if in_ucla = 1 then do;
			if bsi_icteric ne 1 and (uclacod2 = 1 or probcod2 = 1) then icteric_source = 4;
			else if bsi_icteric = 1 and (uclacod2 = 1 or probcod2 = 1) then icteric_source = 5;
			else if bsi_icteric = 1 then icteric_source = 1;
			else icteric_source = 6;
		end;
		
		if lipemic = 0 then lipemic_source = 0;
		else if in_ucla ne 1 then do;
			if bsi_lipemic = 1 and (probcod3 ne 1) then lipemic_source = 1;
			else if (probcod3 = 1) and bsi_lipemic ne 1 then lipemic_source = 2;
			else if probcod3 = 1 and bsi_lipemic = 1 then lipemic_source = 3;
			else lipemic_source = 6;
		end;
		else if in_ucla = 1 then do;
			if bsi_lipemic ne 1 and (uclacod3 = 1 or probcod3 = 1) then lipemic_source = 4;
			else if bsi_lipemic = 1 and (uclacod3 = 1 or probcod3 = 1) then lipemic_source = 5;
			else if bsi_lipemic = 1 then lipemic_source = 1;
			else lipemic_source = 6;
		end;
		
		partial_fill = 0;
		if in_ucla = 1 and uclacod4 = 1 then partial_fill = 1;
		if probcod4 = 1 then partial_fill = 1;
	
		if seq_num in ('0003','0004','0009','0010') then output vial_problems2;		
		else output vial_problems;
	run;
	
	
	proc sort data=vial_problems2 tagsort; by sampleid;
	data vial_problems_seqnum;
		set vial_problems2;
		by sampleid;
		
		retain hemo3 hemo4 icteric3 icteric4 lipemic3 lipemic4 hemo9 hemo10 icteric9 icteric10 lipemic9 lipemic10 partial_fill3 partial_fill4 partial_fill9 partial_fill10 hemo_grade3 hemo_grade4 hemo_grade9 hemo_grade10;
		if first.sampleid then do;
			hemo3 = 0;
			hemo4 = 0;
			icteric3 = 0;
			icteric4 = 0; 
			lipemic3 = 0; 
			lipemic4 = 0;
			hemo9 = 0;
			hemo10 = 0;
			icteric9 = 0;
			icteric10 = 0;
			lipemic9 = 0;
			lipemic10 = 0;
			partial_fill3 = 0;
			partial_fill4 = 0;
			partial_fill9 = 0;
			partial_fill10 = 0;
			hemo_grade3 = 0;
			hemo_grade4 = 0;
			hemo_grade9 = 0;
			hemo_grade10 = 0;
		end;
		
		if seq_num in ('0003') then do;
			hemo3 = hemo;
			icteric3 = icteric;
			lipemic3 = lipemic;
			partial_fill3 = partial_fill;
			hemo_grade3 = hemo_grade;
		end;
		if seq_num in ('0004') then do;
			hemo4 = hemo;
			icteric4 = icteric;
			lipemic4 = lipemic;
			partial_fill4 = partial_fill;
			hemo_grade4 = hemo_grade;
		end;
		if seq_num in ('0009') then do;
			hemo9 = hemo;
			icteric9 = icteric;
			lipemic9 = lipemic;
			partial_fill9 = partial_fill;
			hemo_grade9 = hemo_grade;
		end;
		if seq_num in ('0010') then do;
			hemo10 = hemo;
			icteric10 = icteric;
			lipemic10 = lipemic;
			partial_fill10 = partial_fill;
			hemo_grade10 = hemo_grade;
		end;
					
		if last.sampleid then output;
	run;
	
	
	data vial_problems_seqnum;
		merge vial_problems_seqnum vial_problems2;
		by sampleid;
		
		if seq_num in ('0003') then do;
			if hemo3 ne 1 and hemo4 = 1 then do; hemo = 1; hemo_source = 6; hemo_grade = hemo_grade4 + 10; end;
			if icteric3 ne 1 and icteric4 = 1 then do; icteric = 1; icteric_source = 6; end;
			if lipemic3 ne 1 and lipemic4 = 1 then do; lipemic = 1; lipemic_source = 6; end;
			if partial_fill3 ne 1 and partial_fill4 = 1 then do; partial_fill = 1; partial_fill_source = 6; end;
		end;
		if seq_num in ('0004') then do;
			if hemo4 ne 1 and hemo3 = 1 then do; hemo = 1; hemo_source = 6; hemo_grade = hemo_grade3 + 10; end;
			if icteric4 ne 1 and icteric3 = 1 then do; icteric = 1; icteric_source = 6; end;
			if lipemic4 ne 1 and lipemic3 = 1 then do; lipemic = 1; lipemic_source = 6; end;
			if partial_fill4 ne 1 and partial_fill3 = 1 then do; partial_fill = 1; partial_fill_source = 6; end;
		end;
		
		if seq_num in ('0009') then do;
			if hemo9 ne 1 and hemo10 = 1 then do; hemo = 1; hemo_source = 6; hemo_grade = hemo_grade10 + 10; end;
			if icteric9 ne 1 and icteric10 = 1 then do; icteric = 1; icteric_source = 6; end;
			if lipemic9 ne 1 and lipemic10 = 1 then do; lipemic = 1; lipemic_source = 6; end;
			if partial_fill9 ne 1 and partial_fill10 = 1 then do; partial_fill = 1; partial_fill_source = 6; end;
		end;
		if seq_num in ('0010') then do;
			if hemo10 ne 1 and hemo9 = 1 then do; hemo = 1; hemo_source = 6; hemo_grade = hemo_grade9 + 10; end;
			if icteric10 ne 1 and icteric9 = 1 then do; icteric = 1; icteric_source = 6; end;
			if lipemic10 ne 1 and lipemic9 = 1 then do; lipemic = 1; lipemic_source = 6; end;
			if partial_fill10 ne 1 and partial_fill9 = 1 then do; partial_fill = 1; partial_fill_source = 6; end;
		end;		
	run;
	
	proc sort data=vial_problems_seqnum tagsort; by bsi_id;
	data vial_problems;
		merge vial_problems (keep= bsi_id hemo lipemic icteric hemo_source icteric_source lipemic_source partial_fill hemolyzd hemo_grade specimen_char1-specimen_char5)
					vial_problems_seqnum (keep= bsi_id hemo lipemic icteric hemo_source icteric_source lipemic_source partial_fill hemolyzd hemo_grade specimen_char1-specimen_char5) ;
		by bsi_id;
	run;
	
	data vial;
		merge vial
					vial_problems (keep=bsi_id hemo lipemic icteric hemo_source icteric_source lipemic_source partial_fill hemolyzd hemo_grade specimen_char1-specimen_char5);
		by bsi_id;
	run;
	
	
		
****************************************************************;
****************************************************************;
****************************************************************;
**************** Inherit Flaws from Parents ********************;
****************************************************************;
****************************************************************;
****************************************************************;
****************************************************************;
	
		proc sort data=vial tagsort; by bsi_id;
		data parents children;
			set vial;
			by bsi_id;
			
			if is_parent = 1 then output parents;
			else do;
				if not missing(source_id) then all_parent = source_id;
				else if not missing(parent) then all_parent = parent;
				
				if missing(all_parent) then flag_missing = 1;
				output children;
			end;
		run;
		

		proc sort data=children tagsort; by all_parent;
		data inherit_vials;
			merge children (in=inchild keep= bsi_id all_parent parent hemo flag_missing lipemic icteric plastic tests thaws warning1-warning10 arrived_thawed DNA_usability plastic_tube special_pop in_MSC
																			 robot_popoff_tube remp reposid thawed hemo_grade hemo_source lipemic_source icteric_source 
											rename= (bsi_id = child_bsi_id all_parent = bsi_id))
						parents (in=inparent keep=bsi_id hemo lipemic icteric plastic tests thaws warning1-warning10 arrived_thawed DNA_usability plastic_tube special_pop in_MSC robot_popoff_tube remp
																			reposid thawed hemo_grade hemo_source lipemic_source icteric_source 
																rename=(hemo = parent_hemo lipemic = parent_lipemic icteric = parent_icteric plastic = parent_plastic tests = parent_tests warning1-warning10 = parent_warning1-parent_warning10 
																				arrived_thawed = parent_arrived_thawed DNA_usability = parent_DNA_usability plastic_tube = parent_plastic_tube special_pop = parent_special_pop 
																				in_MSC = parent_in_MSC robot_popoff_tube = parent_robot_popoff_tube remp = parent_remp reposid = parent_reposid thawed = parent_thawed
																				thaws = parent_thaws hemo_grade = parent_hemo_grade 
																				hemo_source = parent_hemo_source lipemic_source = parent_lipemic_source icteric_source = parent_icteric_source));
			by bsi_id;
			if inchild;
			
			in_child = inchild;
			in_parent = inparent;
			
			drop bsi_id;
		run;
	
		
		proc sort data=inherit_vials tagsort; by child_bsi_id;
		data vial;
			merge vial
						inherit_vials (in=inchild rename=(child_bsi_id = bsi_id));
			by bsi_id;
			
			warning_num = 10;
			array a_warning[1:10] warning1-warning10;
			
			if inchild then do;
				if (hemo = 0 and parent_hemo in (1,9)) or (hemo = 9 and parent_hemo in (1)) then do;
					hemo = parent_hemo;
					hemo_grade = parent_hemo_grade;
					hemo_source = parent_hemo_source;
				end;
				if lipemic = 0 and parent_lipemic in (1) then do;
					lipemic = parent_lipemic;
					lipemic_source = parent_lipemic_source;
				end;
				if icteric = 0 and parent_icteric in (1) then do;
					icteric = parent_icteric;
					icteric_source = parent_icteric_source;
				end;
				if plastic = 0 and parent_plastic in (1) then plastic = parent_plastic;
				if plastic_tube = 0 and parent_plastic_tube in (1) then plastic_tube = parent_plastic_tube;
				if tests = 0 and parent_tests > 0 then tests = parent_tests;
				if arrived_thawed = 0 and parent_arrived_thawed in (1) then arrived_thawed = parent_arrived_thawed;
			end;
			
			
			*** perfect and flaw_status;
			perfect_warning = 0;
			flaw_warning = 0;
			do i = 1 to warning_num;
				if a_warning[i] in ('A','B','C5','CR','CV','FO','H','LV','ML','NL','OI','QM','QT','QV','VB','R','T','TN','E','RB','RE','QS','VD') then perfect_warning = 1;
				if a_warning[i] in ('A','B','C5','CR','CV','FO','H','LV','ML','NL','QM','QT','QV','VB','QS','VD') then flaw_warning0 = 1;
				if a_warning[i] in ('C1','R','RB','RE','T','TN','TI','E') then flaw_warning1 = 1;
				if a_warning[i] in ('OI') then flaw_warning2 = 1;
			end;
		
			if vialstat in ('1','3','7','E','P','S','2') and full = 1 and thawed = 0 and hemo in (0,9) and tests = 0 and in_MSC = 0 and plastic_tube = 0 and plastic ne 1 and icteric ne 1 and lipemic ne 1 
				and robot_popoff_tube ne 1 and arrived_thawed ne 1 and perfect_warning ne 1 and reposid ne 'U' and not (full = 0 and is_parent = 1) and flaw_warning0 ne 1 and flaw_warning1 ne 1 
				then perfect = 1;
			else if vialstat in ('1','3','7','E','P','S','2') and thawed = 0 and hemo in (0,9) and tests = 0 and perfect_warning ne 1 and in_MSC = 0 and plastic_tube ne 1 and plastic ne 1 and parent ne ' ' and volume ne 0 
			  and icteric ne 1 and lipemic ne 1 and robot_popoff_tube ne 1 and arrived_thawed ne 1 and reposid ne 'U' and not (full = 0 and is_parent = 1) and flaw_warning0 ne 1 and flaw_warning1 ne 1 
				then perfect = 1;
			else perfect = 0; 
			
			if flaw_warning0 = 1 or vialstat in ('4','5','6','X','Y') then flaw_status = 0;
			else if icteric = 1 or arrived_thawed = 1 or flaw_warning1 = 1 then flaw_status = 1;
			else if plastic = 1 or tests > 0 or plastic_tube = 1 or in_msc = 1 or robot_popoff_tube = 1 or reposid = 'U' or flaw_warning2 = 1 then flaw_status = 2;
			else if ((hemo = 1 or lipemic = 1) and thawed = 1) then flaw_status = 3;
			else if thawed = 1 then flaw_status = 4;
			else if hemo = 1 or lipemic = 1 then flaw_status = 5;
			else if full = 0 and is_parent = 1 then flaw_status = 7;
			else if perfect = 1 then flaw_status = 8;
			else flaw_status = .M;	
			     
			
			if in_ucla = 1 then do;
				if vialstat in ('1','3','7','E','P','S','2') and full = 1 and thawed = 0 and hemo in (0,9) and in_MSC = 0 and plastic_tube = 0 and plastic ne 1 and icteric ne 1 and lipemic ne 1 
					and robot_popoff_tube ne 1 and arrived_thawed ne 1 and perfect_warning ne 1 and not (full = 0 and is_parent = 1) and flaw_warning0 ne 1 and flaw_warning1 ne 1 
					then perfectu = 1;
				else if vialstat in ('1','3','7','E','P','S','2') and thawed = 0 and hemo in (0,9) and perfect_warning ne 1 and in_MSC = 0 and plastic_tube ne 1 and plastic ne 1 and parent ne ' ' and volume ne 0 
				  and icteric ne 1 and lipemic ne 1 and robot_popoff_tube ne 1 and arrived_thawed ne 1 and not (full = 0 and is_parent = 1) and flaw_warning0 ne 1 and flaw_warning1 ne 1 
					then perfectu = 1;
				else perfectu = 0; 
			
				if flaw_warning0 = 1 or vialstat in ('4','5','6','X') then ucla_condition = 0;
				else if icteric = 1 or arrived_thawed = 1 or flaw_warning1 = 1 then ucla_condition = 1;
				else if plastic = 1 or tests > 1 or plastic_tube = 1 or in_msc = 1 or robot_popoff_tube = 1 then ucla_condition = 2;
				else if ((hemo = 1 or lipemic = 1) and thawed = 1) then ucla_condition = 3;
				else if thawed = 1 then ucla_condition = 4;
				else if hemo = 1 or lipemic = 1 then ucla_condition = 5;
				else if full = 0 then ucla_condition = 7;
				else if perfectu = 1 then ucla_condition = 8;
				else ucla_condition = 2;	
			end;
			else ucla_condition = .N;
			
    
    
			*** buccal cells do not have date draw. Child DNA shouldn't either;
			*if source_mat = 'CH' and mattype = 'CB' then bsi_dt_draw = .N;
			
			drop parent_hemo parent_lipemic parent_icteric parent_plastic parent_plastic_tube parent_tests parent_arrived_thawed perfect_warning flaw_warning0 flaw_warning1 
				flaw_warning2 robot_popoff_tube plastic_tube 
				perfect_warning parent_warning1-parent_warning10 parent_DNA_usability parent_special_pop parent_in_MSC parent_robot_popoff_tube parent_remp  
				parent_reposid parent_thawed parent_hemo_grade parent_hemo_source parent_icteric_source parent_lipemic_source flag_missing parent_thaws in_child in_parent i warning_num flaw_warning perfectu;
		run;
		
		
		
	proc freq data=lab_reqs;
		title 'lab reqs dataset';
		table req_status * req_vial_status * req_reason * req_vial_reason /list missing;
	run;
	
	proc freq data=vial;
		title 'final vial dataset';
		table req_status * req_vial_status * req_reason * req_vial_reason /list missing;
	run;
		  
		  


****************************************************************;
****************************************************************;
****************************************************************;
***** Set up Variables about Mulitple DNA Vials at Central *****;
****************************************************************;
****************************************************************;
****************************************************************;
****************************************************************;

	proc sort data=vial; by bsi_id;
	*** one record per sampleid, study year, extraction method;
	data dna_vials;
		set vial; 
		by bsi_id;
		if mattype = 'CB' and vialstat in ('1','3','7','S','E') and not missing(dna_extract_mthd) and reposid = 'E';
	run;
		
	*** count multiple vial;
	proc sort data=dna_vials; by subject sampleid study_yr source_mat gsa_dna dna_extract_mthd dt_enter;
	data dna_vials_vial_count;
		set dna_vials;
		by subject sampleid study_yr source_mat gsa_dna dna_extract_mthd dt_enter;
		
		length dna_multiple_vials_sampleid1-dna_multiple_vials_sampleid3 $ 7;
		
		retain dna_multiple_vials_overall dna_multiple_vials_num1-dna_multiple_vials_num3 dna_multiple_vials_set_ids dna_multiple_vials_sampleid1-dna_multiple_vials_sampleid3 
					 dna_multiple_vials_dt_enter1-dna_multiple_vials_dt_enter3;
		
		array a_dna_multiple_vials_num[1:3] dna_multiple_vials_num1-dna_multiple_vials_num3;
		array a_dna_multiple_vials_sampleid [1:3] dna_multiple_vials_sampleid1-dna_multiple_vials_sampleid3;
		array a_dna_multiple_vials_dt_enter[1:3] dna_multiple_vials_dt_enter1-dna_multiple_vials_dt_enter3;
		
		
		if first.subject then do;
			dna_multiple_vials_set_ids = 0;
			dna_multiple_vials_overall = 0;
			do i = 1 to 3;
				a_dna_multiple_vials_num[i] = 0;
				a_dna_multiple_vials_sampleid[i] = '';
				a_dna_multiple_vials_dt_enter[i] = .N;
			end;
		end;
		
		if first.dt_enter then dna_multiple_vials_overall = 0;
		dna_multiple_vials_overall = dna_multiple_vials_overall + 1;
		
		
		if last.dt_enter and dna_multiple_vials_overall > 1 then do;
			dna_multiple_vials_set_ids = dna_multiple_vials_set_ids + 1;	
			a_dna_multiple_vials_sampleid[dna_multiple_vials_set_ids] = sampleid;
			a_dna_multiple_vials_dt_enter[dna_multiple_vials_set_ids] = dt_enter;
			a_dna_multiple_vials_num[dna_multiple_vials_set_ids] = dna_multiple_vials_overall;
		end;
		
		if last.subject then output;
	run;
	
	
	proc sort data=dna_vials_vial_count; by subject;
	proc sort data=vial; by subject;
	
	data vial;
		merge vial (in=innew)
					dna_vials_vial_count (in=indna keep=subject dna_multiple_vials_num1-dna_multiple_vials_num3 dna_multiple_vials_set_ids dna_multiple_vials_sampleid1-dna_multiple_vials_sampleid3 
																dna_multiple_vials_dt_enter1-dna_multiple_vials_dt_enter3);
		by subject;
		
		
		array a_dna_multiple_vials_num[1:3] dna_multiple_vials_num1-dna_multiple_vials_num3;
		array a_dna_multiple_vials_sampleid [1:3] dna_multiple_vials_sampleid1-dna_multiple_vials_sampleid3;
		array a_dna_multiple_vials_dt_enter[1:3] dna_multiple_vials_dt_enter1-dna_multiple_vials_dt_enter3;
		
		dna_multiple_vials = 0;
		dna_multiple_vials_set_id = 0;
		dna_multiple_vials_set_num = 0;
		
		if mattype = 'CB' and vialstat in ('1','3','7','S','E') and not missing(dna_extract_mthd) and reposid = 'E' and in_msc ^= 1 then do i = 1 to 3;
			if sampleid = a_dna_multiple_vials_sampleid[i] and dt_enter = a_dna_multiple_vials_dt_enter[i] then do;
				dna_multiple_vials = 1;
				dna_multiple_vials_set_id = i;
				dna_multiple_vials_set_num = a_dna_multiple_vials_num[i];
			end;
		end;
		
		label dna_multiple_vials = 'DNA at Central Repository with Multiple Vials'
					dna_multiple_vials_set_id = 'DNA at Central Repository with Multiple Vials - Set ID'
					dna_multiple_vials_set_num = 'DNA at Central Repository with Multiple Vials - Number in Set';
		
		drop i dna_multiple_vials_num1-dna_multiple_vials_num3 dna_multiple_vials_sampleid1-dna_multiple_vials_sampleid3 dna_multiple_vials_dt_enter1-dna_multiple_vials_dt_enter3 dna_multiple_vials_set_ids;
	run;


****************************************************************;
****************************************************************;
****************************************************************;
****** CDCC Buccal Comments Needed for  Sample File ************;
****************************************************************;
****************************************************************;
****************************************************************;
****************************************************************;
	proc sort data=all_vials tagsort; by sampleid;
	data buccal_comments (keep=sampleid buccal_comments);
		set all_vials;
		by sampleid;
		if not missing(buccal_comments);
	run;


****************************************************************;
****************************************************************;
****************************************************************;
****** DESL Info Needed for Sample and Person Files ************;
****************************************************************;
****************************************************************;
****************************************************************;
****************************************************************;

proc sort data=vial tagsort; by sampleid;

data vial_summary;
	set vial (rename=(desl_sampleid_usable = vial_desl_sampleid_usable desl_sampleid_unusable = vial_desl_sampleid_unusable));
	by sampleid;
	
	retain desl_sampleid_usable desl_sampleid_unusable;
	if first.sampleid then do;
		desl_sampleid_usable= 0;
		desl_sampleid_unusable = 0;
	end;
	
	if vial_desl_sampleid_usable = 1 then desl_sampleid_usable = 1;
	if vial_desl_sampleid_unusable = 1 then desl_sampleid_unusable = 1;
	
	if last.sampleid;
run;

data sampleid_vials;
	merge sampleid_vials
				vial (keep= sampleid pid subject study_yr desl_sampleid_usable desl_sampleid_unusable)
				unusedna (keep=sampleid in_desl_flagged_returns);
	by sampleid;
run;

proc sort data=all_vials tagsort; by sampleid;
data sampleid_vials;
	merge sampleid_vials
				all_vials (keep= sampleid rnddate);
	by sampleid;
run;

data sampleid_vials;
	merge sampleid_vials
				buccal_comments;
	by sampleid;
run;


proc freq data=sampleid_vials;
	table in_desl_flagged_returns /list missing;
run;



proc cport data=sampleid_vials file=sampdesl;
	

data vial;
	set vial (drop=desl_sampleid_usable desl_sampleid_unusable);
run;		


%set_labels(dataset=vial, spreadsheet=/prj/plcoims/database/lab_specimen/blood/progs/vamus/blood_excel/current_vial.dictionary.d&format_date..xls);

data select_vial all_vials;
	set vial;
	
	if vialstat in ('1','3','7','S','E') then output select_vial;
	output all_vials;
run;


proc cport data=select_vial file=vialfile;
proc cport data=all_vials file=allfile;