merge into DWH.DWTB_KV_BEST_FAHRZEUG_ZUNA_S1 s1
/* source data */
using (
    select * from DWH.DWTB_KV_BEST_FAHRZEUG_ZUNA_S0 where FZUNAART is not null and FZUNAPBK is not null
) s0
/* key condition */
on (s1.VSNR = s0.VSNR and 
    s1.VSNR_DAT = s0.VSNR_DAT and 
    s1.F_ZUNA_SCHL = s0.FZUNASCHL and 
    s1.F_ZUNA_SCHL_LFD_NR = s0.FZUNASCHLLFDNR
)
/* key condition: new datasets */
when not matched then
/* insert new datasets */
insert (
  GUELTIG_VON,
  GUELTIG_BIS,
  PROZESS_ID_ANLAGE,
  VSNR,
  VSNR_DAT,
  F_ZUNA_BAUSTEIN_ART,
  F_ZUNA_ART,
  F_ZUNA_PBK,
  F_ZUNA_BETR,
  F_ZUNA_SCHL,
  F_ZUNA_SCHL_LFD_NR,
  F_ZUNA_POS,
  CHECKSUM)
values (
    s0.STAND_DATUM,
    ?/*#1  SCD_END */,
    ?/*#2  PROZESS_ID_ANLAGE */,
    s0.VSNR,
    s0.VSNR_DAT,
    s0.FZUNABAUSTEINART,
    s0.FZUNAART,
    s0.FZUNAPBK,
    s0.FZUNABETR,
    s0.FZUNASCHL,
    s0.FZUNASCHLLFDNR,
    s0.FZUNA_POS,
    s0.CHECKSUM   
 )
/* key condition: changed datasets */
when matched 
	and s1.CHECKSUM <> s0.CHECKSUM 
then
/* changed datasets */
update 
    for portion of business_time 
        from ?/*#3  STAND_DATUM */
	to ?/*#4  SCD_END */
set PROZESS_ID_AEND=?/*#5  PROZESS_ID_ANLAGE */,
    s1.VSNR = s0.VSNR,
    s1.VSNR_DAT = s0.VSNR_DAT,
    s1.F_ZUNA_BAUSTEIN_ART = s0.FZUNABAUSTEINART,
    s1.F_ZUNA_ART = s0.FZUNAART,
    s1.F_ZUNA_PBK = s0.FZUNAPBK,
    s1.F_ZUNA_BETR = s0.FZUNABETR,
    s1.F_ZUNA_SCHL = s0.FZUNASCHL,
    s1.F_ZUNA_SCHL_LFD_NR = s0.FZUNA_POS,
    s1.F_ZUNA_POS = s0.FZUNA_POS,
    s1.CHECKSUM = s0.CHECKSUM