# Scilifelab_epps Version Log

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
