# Scilifelab_epps Version Log

## 20230630.1
Implement ONT and Anglerfish samplesheet generation for MinION QC.

## 20230622.1
Bugfix for deviation 173. Differentiate metadata paths for Illumina instruments.

## 20230615.1
Put generated ONT samplesheets on ngi-nas-ns instead of mfs.

## 20230613.1
Rework zika_utils.format_worklist() split transfers logic to prevent the post-split volume from ending up as less than what is allowed by the instrument.

## 20230602.1
Rename utils module to epp_utils to avoid name collision with native Python module and fix bug causing fatal error for Zika pooling.

## 20230529.1
Assign step (accidentally omitted from PR #150) to RN config.

## 20230525.1
Live troubleshooting of ONT EPPs upon deployment of new workflow to LIMS prod.

## 20230329.1
Improve modularity and readability of ONT EPP script names and contents. Also implement changes requested during live testing.

## 20230313.1
Deploy validation 23_02_zika_codebase_revamp to replace accredited codebase for pooling using Mosquito X1.

## 20230306.2
Update control lists and fetch run recipe from project for samplesheet generator

## 20230306.1
Replace formula used for ng -> molar conversion.

## 20230227.1
Improvements and bugfixes on ONT EPPs.

## 20230224.2
Add four new EPPs related to the updated ONT workflow deploying shortly.

## 20230224.1
Update after live troubleshooting of new Zika pooling code. Fix faulty variable name and improve error logging.

## 20230222.1
Support nM as a valid conc unit for Aggregate QC DNA and RNA

## 20230213.1
Differentiate Zika normalization parameters for Amplicon workflow plate set-up. Unlike QIAseq and SMARTer it should use customer metrics and a lower minimum volume.

## 20230209.1
Enable verify index and placement epp for checking wrong well format

## 20230207.1
Update 20230130.2, correct the volume and conc information that is fetched and support both nM and ng/ul pooling. General updates to make the code simpler and more maintainable.

## 20230130.2
zika_refactoring
Add re-factored pooling code for Zika. Re-route to the new code ONLY for RAD-seq pooling (non-accredited). Accredited operations will run on the old code, for now.

## 20230130.1
Convert 10X dual index 2 to RC for MiSeq

## 20230128.1
Update index_checker EPP to support SMARTSEQ3 indexes

## 20230126.1
Fix issue with NaN values for fragment analyzer results

## 20230123.1
Fix bug that manual in UDF instrument is recorded in logbook

## 20230116.1
Refactor EPP scripts for qc_amount_calculation

## 20221215.1
When writing the Zika deck layout in a worklist comment, omit all commas, to prevent the line from being cut-off.

## 20221123.1
New EPP for calculating cell or nuclei conc for the new 10X Chromium workflow

## 20221122.1
Also support two new UDFs for the QIAseq miRNA and Amplicon workflows for Bravo

## 20221121.1
Large update in functionality of Zika code. Accomodate two new UDFs and enable usage in the non-validated methods SMARTer PicoRNA, QIAseq miRNA and amplicon normalization.

## 20221116.2
Refactor of the default_bravo and calc_vol functions for bravo_csv to include two new UDFs

## 20221116.1
Update amount taken and total volume for bravo_csv

## 20221109.1
Implement Zika for QIAseq setup and start refactoring Zika code into separate files zika.py and zika_methods.py

## 20221011.1
Fix bug that manual in UDF instrument is recorded in logbook

## 20220914.1
Multiple EPP changes to support the OmniC protocol v2.0

## 20220909.1
Handle special characters in PCs name

## 20220907.1
Add more optional keys for Aggregate QC

## 20220904.1
Add PromethION Sequencing in comments_to_running_notes

## 20220902.2
Fix bug with index checker with submitted container info for inhouse libraries

## 20220902.1
New EPP for copying input UDF to output

## 20220831.1
For MiSeq samplesheet, replace Experiment Name with Flowcell ID

## 20220804.1
Add new control types for samplesheet generator

## 20220722.1
Upgrade index checker to throw error for bad format indexes

## 20220718.1
Fix bug with manage_demux_stats that noindex case cannot be handled for NextSeq

## 20220709.2
Upgrade index checker for checking sample placement

## 20220709.1
Fix issue that record changes EPP cannot handle controls

## 20220708.1
Refactor index checker for better handling of smartseq indexes

## 20220707.1
Write verify indexes comments to running notes

## 20220706.1
Upgrade index checker for verifying finished library projects

## 20220701.1
Fix bug with single read MiSeq run for illumina_run_parameter_parser

## 20220630.1
Make a new logbook EPP based on Google service account

## 20220629.1
Support Biomek for logbook

## 20220628.1
Remove workset tag for CaliperGX in comments_to_running_notes

## 20220616.1
Fix path of QC_criteria.json

## 20220615.1
Update statusdb URL to use https

## 20220608.1
Fix index distance checker for cases that one sample with multiple indexes

## 20220606.1
Fix samplesheet generator for cases that one sample with multiple indexes

## 20220602.1
Rename FC and cartridge UDFs for NextSeq and add NextSeq 2000 P1

## 20220506.1
Take 2uL sample for low pipetting volume cases for the SMARTer Pico RNA workflows

## 20220503.1
Include controls in samplesheet for MiSeq, NextSeq and NovaSeq

## 20220428.1
Enable illumina_run_parameter_parser for parsing run stats for NovaSeq

## 20220427.1
Support 10X SI-TS indexes

## 20220415.2
New EPP for summarizing Aggregate QC stats into running notes, stats for QC metrics

## 20220415.1
New EPP for summarizing Aggregate QC stats into running notes

## 20220412.1
Refactor 10X index pattern names

## 20220409.1
Do not convert index 2 for finished library samples for MiSeq

## 20220407.1
New index handling method for samplesheet generator

## 20220313.1
Update illumina_run_parameter_parser for handling MiSeq run without index cycles

## 20220304.1
Multiple EPP changes to support the new OmniC protocol

## 20220301.1
Support Mosquito for logbook

## 20220222.1
Return message when no issue detected for index checker

## 20220221.2
Refactor index checker to support 10X indexes

## 20220221.1
New EPP for checking index distance

## 20220217.1
Update illumina_run_parameter_parser for parsing run stats for MiSeq

## 20220215.1
Put back Workflow for samplesheet generator for MiSeq

## 20220211.1
Replace UDF for samplesheet generator for MiSeq

## 20220202.1
Update to send email to proj coord when a running note is written from LIMS

## 20211104.1
Update samplesheet generator to handle non-QC Minion sequencing step

## 20211027.1
Remove FastQ path from MinION samplesheet

## 20211025.2
Bravo CSV EPP for new library normalization and pooling steps

## 20211025.1
EPP support for new library normalization and pooling steps

## 20211021.1
Show ERROR messages when pool volume is too high

## 20211013.1
Support selectable Fragment Analyzer for logbook

## 20211011.1
Update anglerfish results parser to support outputfile with new format

## 20211007.1
Support fmol amount calculation

## 20210930.1
Fix bug with control samples for bravo_csv

## 20210920.1
Exclude RNA no depletion protocol from volume adjustment

## 20210910.1
Update bravo_csv to support volume adjustment for high conc samples

## 20210809.1
Update threshold of max undet per lane percentage for demux step

## 20210702.1
Upgrade EPPs to support the new ONT protocol

## 20210617.1
Support additional 10X index types in samplesheet generator
Update 10X index list

## 20210615.1
Support DV200 for Caliper result parser

## 20210603.1
Allow empty path for Minion QC

## 20210531.1
Fix bug with MiSeq in samplesheet generator

## 20210528.1
Better sort functions for bravo csv and samplesheet

## 20210525.2
Fix issue with error message

## 20210525.1
Add fragment analyzer protocols in comments_to_running_notes

## 20210520.1
Upgrade EPPs to support the new QIAseq miRNA protocol

## 20210519.1
Fix bug with None type comparison in copy_qubit.py

## 20210511.1
Update obtain_customer_cc.py to support custom volume

## 20210503.1
Update scripts for parsing fragment analyzer result files

## 20210419.1
Port scripts to python 3

## 20210414.1
Update illumina_run_parameter_parser for parsing run stats

## 20210410.1
Update samplesheet generator to handle blanks in sample index

## 20210409.2
Update EPP for parsing run info for NextSeq 2000, MiSeq and NovaSeq

## 20210409.1
Update EPP for parsing run info for both NextSeq 2000 and MiSeq

## 20210408.1
New EPP for parsing run info for NextSeq 2000

## 20210313.1
Support additional 10X index types in samplesheet generator
Update 10X index list

## 20210226.1
Change plate name to plate id for Bravo CSV for qPCR

## 20210224.2
Add new EPP for aliquoting samples for qPCR steps

## 20210224.1
Setup VERSIONLOG.md
