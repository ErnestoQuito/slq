
--1- corres maestro de equipos

use Movistar_104_v2

--PRIMERO MAESTRO

SET language spanish  
DELETE FROM T_Maestro104Temporal  

--ejecutar para insertar del procedimiento a la tabla temporal de activos, sin fecha.

insert into T_Maestro104Temporal   
execute CDG_PERU.BDSQL.uSi_uSp_Maestro_Equipos 'movistar perú','07/09/2017' ,'0'   


--Insert de la tabla temporal a la tabla de Maestro 104 con fecha.
--para reportes

insert into T_MaestroUsuarios104 
select * from (select    
      FECHA='                       ',  
      DNI,  
      [RACS STC],  
      [Usuario SRM],  
      [CTI],
      uWindows, 
      [Nombre],
      [IngresoAllus],  
      [Ingreso a Línea],
      [Id_Mitrol]
 from T_Maestro104Temporal)A

--ejecutar el update

UPDATE T_MaestroUsuarios104  
SET FECHA='2017-09-07 00:00:00.000'   
WHERE FECHA=''  

delete T_V_Maestrohistorico104
where Fecha>='20170901' 
and Cargo in ('REPRESENTANTE                                     ',
'BACKOFFICE                                        ',
'TRAINNING'

)

set language english
insert into T_V_Maestrohistorico104 select * from V_MaestroHistoricoSuperiores104
where Fecha>='20170901'
and Cargo in ('REPRESENTANTE                                     ',
'BACKOFFICE                                        ',
'TRAINNING')

--2- Importo BIG BANG DE NEGOCIOS,PLATINO,RESIDENCIAL,CORTE POR ROBO IN Y CORTE POR ROBO OUT.

PLATINO ABRIL 2017 -> PLATINOABRIL2017
NEGOCIOS ABRIL 2017 -> NEGOCIOSABRIL2017
RESIDENCIAL ABRIL 2017 -> RESIDENCIALABRIL2017
CORTE POR ROBO IN -> CORTEPORROBOIN
CORTE POR ROBO OUT -> CORTEPORROBOOUT


select * from RESIDENCIALABRIL2017 where [Fecha y Hora] >= '20170901'


UPDATE RESIDENCIALABRIL2017 SET [Estado 2] = 'DOWSELL'
WHERE [Estado 2] = 'VENTA DOWSELL' and [Fecha y Hora] >= '20170901'

UPDATE RESIDENCIALABRIL2017 SET [Estado 2] = 'UPSELL'
WHERE [Estado 2] = 'VENTA UPSELL' and [Fecha y Hora] >= '20170901'

UPDATE RESIDENCIALABRIL2017 SET [Estado 2] = 'MANTIENE'
WHERE [Estado 2] = 'VENTA MANTIENE' and [Fecha y Hora] >= '20170901'

-------
UPDATE RESIDENCIALAGOSTO2017 SET [Estado 2] = 'DOWSELL'
WHERE [Estado 2] = 'VENTA DOWSELL' and [Fecha y Hora] >= '20170901'

UPDATE RESIDENCIALAGOSTO2017 SET [Estado 2] = 'UPSELL'
WHERE [Estado 2] = 'VENTA UPSELL' and [Fecha y Hora] >= '20170901'

UPDATE RESIDENCIALAGOSTO2017 SET [Estado 2] = 'MANTIENE'
WHERE [Estado 2] = 'VENTA MANTIENE' and [Fecha y Hora] >= '20170901'

--3- DESCARGAMOS AGENT Y AUSENTISMO.
--4- PROCEDIMIENTO PARA INTEGRADO:


--borrar ausentismo del mes--

delete from [Ausentismo Plano] where Fecha >='20170901' and Fecha < '20171001'

select * from  [Ausentismo Plano] where Fecha >='20170901'

--importar el ausentismo y el detalle de agentes--

--borrar la tabla final--

DELETE TAB_V_BASE_INTEGRADO_INFOMART WHERE FECHA > = '20170901' and FECHA < '20171001'

--insertar los datos del mes en la tabla--

set language english
insert into TAB_V_BASE_INTEGRADO_INFOMART select * from v_base_integrado_infomart 
where FECHA > = '20170901' and FECHA < '20171001'


select * from TAB_V_BASE_INTEGRADO_INFOMART where YEAR(fecha)='2017' and MONTH(fecha)='09' and DAY(fecha)='30' 
--5- IMPORTAMOS AGENTS 30 MINUTOS, DESCARGAS INFORMART Y SRM(MOVISTAR_104)

select top 10* from Agent_Queue_Report_30min where [Day]  like '%2017-06-08%'
--Agent_Summary_Activity_Report_Active_30min
--Agent_Summary_Activity_Report_Interaction_30min

--Abandon_Delay_Report
--Queue_Outline_Report
--Queue_Summary_Report
--Speed_of_Accept_(seconds)_Report
--Interaction_Handling_Attempt

--MOVISTAR_104 -> SRM

--6-- CORREMOS SCRIPT PARA GENERAR TXT DE CONSOLIDADO:

UPDATE Queue_Outline_Report SET [Queue 2]='41002015_RES_POS_VNT_IVR104_ALLUS_CHICLAYO' where [Queue]='VQ41002015_RES_POS_VNT_IVR104_ALLUS_CHICLAYO'
UPDATE Queue_Outline_Report SET [Queue 2]='41002015_RES_POS_VNT_IVR104_ALLUS_LIMA' where [Queue]='VQ41002015_RES_POS_VNT_IVR104_ALLUS_LIMA'
UPDATE Queue_Outline_Report SET [Queue 2]='42002015_RES_POS_VNT_IVR104_ALLUS_CHICLAYO' where [Queue]='VQ42002015_RES_POS_VNT_IVR104_ALLUS_CHICLAYO'
UPDATE Queue_Outline_Report SET [Queue 2]='42002015_RES_POS_VNT_IVR104_ALLUS_LIMA' where [Queue]='VQ42002015_RES_POS_VNT_IVR104_ALLUS_LIMA'

--BRIO 07
exec SP_Infomart_104_v2 '2017-09-07 00:00:00.000'

--BRIO 09
exec [BRIO9_TXT] '2017-09-06 00:00:00.000'

--TXT 25
select [Object Id]=0, [Start Timestamp] as 'Begin Time', [End Timestamp] as 'End Time',
CONVERT(CHAR(10),case when len(convert(char(2),MONTH([Start Timestamp])))=1 then '0'+convert(char(2),MONTH([Start Timestamp])) else convert(char(2),MONTH([Start Timestamp])) end
+'/'+case when len(convert(char(2),day([Start Timestamp])))=1 then '0'+convert(char(2),day([Start Timestamp])) else convert(char(2),day([Start Timestamp])) end
+'/'+right((convert(char(4),YEAR([Start Timestamp]))),2))
as 'FECHA',
LEFT(convert(char(5),cast(convert(char(10),dateadd([minute], (datediff([minute], '19000101',[Start Timestamp])/30)*30,'19000101'),108)
as datetime),108),2)+right(
convert(char(5),cast(convert(char(10),dateadd([minute], (datediff([minute], '19000101',[Start Timestamp])/30)*30,'19000101'),108)
as datetime),108),2)as intervalo, 'Connid'=0,
[From] as 'Ani','Dnis'=0,
CASE WHEN patrones25.CDN<>'' THEN 'ALLUS' ELSE '' END AS 'CALL CENTER',
h.Servicio as 'PROGRAMA',
patrones25.CDN as 'CDN',
(patrones25.CDN+'-'+[Customer Segment]) AS 'SKILL',
([Customer Engage Time]+ [Customer Hold Time] ) as 'TO',
[Customer Engage Time] as 'Talktime sin Hold' , 
[Customer Hold Time] as 'Hold Time', [Customer Wrap Time] as 'AcwTime',
CASE WHEN ([Customer Engage Time]+ [Customer Hold Time] )<=10 THEN 'TO10SEG' ELSE '' END AS 'TO10seg',
[Aband In Hold]=0,
[Handling Resource] as 'First Login',
'Anexo'=0,[Site Ingreso Llamada]=0,[Site Atenci Llamada]=0,[Key Ivr 02]=0,[Key Ivr 03]=0,[Call Retry]=0,[Key Ivr 04]=0
from Interaction_Handling_Attempt I 
inner join [patrones25] on Patrones25.[desc_cdn lv]=i.[queue 2]
inner join T_V_Maestrohistorico104 h on h.Documento=i.[Handling Resource] 
and h.Fecha=CAST(CONVERT(DATE,[Start Timestamp],103) AS DATETIME)
where CONVERT(time,[Start Timestamp],108)>'6:00:00' and 
 CONVERT(time,[Start Timestamp],108)<'22:59:00' and year([Start Timestamp])='2017' 
 and MONTH([Start Timestamp])='09'
AND DAY([Start Timestamp])='06' AND [Interaction Type]='Inbound'

--select * from Interaction_Handling_Attempt where [Handling Attempt Start] like '%2017-06-07%'
--select * from Interaction_Handling_Attempt where YEAR([Handling Attempt Start])='2017' and MONTH([Handling Attempt Start])='06' and DAY([Handling Attempt Start])='07'
--delete Interaction_Handling_Attempt where YEAR([Handling Attempt Start])='2017' and MONTH([Handling Attempt Start])='06' and DAY([Handling Attempt Start])='07'
------------------------------------------------------------------

--TXT 26

select 'Connid'=0, [From] as 'Ani',[Start Timestamp] as 'Begin Time', [End Timestamp] as 'End Time',
( (left(convert(char(8),[HANDLING ATTEMPT END],108),2))*3600+
(substring(convert(char(8),[HANDLING ATTEMPT END],108),charindex(':',convert(char(8),[HANDLING ATTEMPT END],108),3)+1,2))*60+
right(convert(char(8),[HANDLING ATTEMPT END],108),2))-

((left(convert(char(8),[HANDLING ATTEMPT START],108),2))*3600+
(substring(convert(char(8),[HANDLING ATTEMPT START],108),charindex(':',convert(char(8),[HANDLING ATTEMPT START],108),3)+1,2))*60+
right(convert(char(8),[HANDLING ATTEMPT START],108),2))
as 'T CALL',

 (left(convert(char(8),[End timestamp],108),2))*3600+
(substring(convert(char(8),[End timestamp],108),charindex(':',convert(char(8),[End timestamp],108),3)+1,2))*60+
right(convert(char(8),[End timestamp],108),2)-
( (left(convert(char(8),[Start Timestamp],108),2))*3600+
(substring(convert(char(8),[Start Timestamp],108),charindex(':',convert(char(8),[Start Timestamp],108),3)+1,2))*60+
right(convert(char(8),[Start Timestamp],108),2)) as 'TSALIENTE',

Anexo=0, H.Documento AS 'Login ID', CASE WHEN Documento<>'' THEN 'ALLUS' ELSE '' END AS 'Centro',
left(convert(char(8),[Start Timestamp],108),2) as HORA,
substring(convert(char(8),[Start Timestamp],108),charindex(':',convert(char(8),[Start Timestamp],108),3)+1,2) AS Minutos,
LEFT(convert(char(5),cast(convert(char(10),dateadd([minute], (datediff([minute], '19000101',[Start Timestamp])/15)*15,
'19000101'),108)
as datetime),108),2)+right(
convert(char(5),cast(convert(char(10),dateadd([minute], (datediff([minute], '19000101',[Start Timestamp])/15)*15,'19000101'),108)
as datetime),108),2)as Intervalo, 
LEFT(convert(char(5),cast(convert(char(10),dateadd([minute], (datediff([minute], '19000101',[Start Timestamp])/30)*30,
'19000101'),108)
as datetime),108),2)+right(
convert(char(5),cast(convert(char(10),dateadd([minute], (datediff([minute], '19000101',[Start Timestamp])/30)*30,'19000101'),108)
as datetime),108),2)as [30Min],
convert(char(10),[Start Timestamp],103) as 'FECHA',
convert(char(2),MONTH([Start Timestamp])) as 'N_Mes',
mes=CASE
WHEN month([Start Timestamp])=1 THEN 'Enero'
WHEN month([Start Timestamp])=2 THEN 'Febrero'
WHEN month([Start Timestamp])=3 THEN 'Marzo'
WHEN month([Start Timestamp])=4 THEN 'Abril'
WHEN month([Start Timestamp])=5 THEN 'Mayo'
WHEN month([Start Timestamp])=6 THEN 'Junio'
WHEN month([Start Timestamp])=7 THEN 'Julio'
WHEN month([Start Timestamp])=8 THEN 'Agosto'
WHEN month([Start Timestamp])=9 THEN 'Septiembre'
WHEN month([Start Timestamp])=10 THEN 'Octubre'
WHEN month([Start Timestamp])=11 THEN 'Noviembre'
WHEN month([Start Timestamp])=12 THEN 'Diciembre'
END,
year([Start Timestamp]) as AÑO
from Interaction_Handling_Attempt I 

inner join T_V_Maestrohistorico104 h on h.Documento=i.[Handling Resource] 
and h.Fecha=CAST(CONVERT(DATE,[Start Timestamp],103) AS DATETIME)
where CONVERT(time,[Start Timestamp],108)>'6:00:00' and 
CONVERT(time,[Start Timestamp],108)<'22:59:00' and year([Start Timestamp])='2017' 
and MONTH([Start Timestamp])='09'
AND DAY([Start Timestamp])='06' AND [Interaction Type]='Outbound'

GO



--7 CORTE POR ROBO


insert into BASE_INFOMART_CXR  
exec SP_Infomart_104_v2 '2017-09-07 00:00:00.000'


select top 1* from BASE_INFOMART_CXR 

SP_helpText SP_Informart_104_v2
DELETE PATRONES25
DELETE patrones



use Movistar_104
select * from cxr_rnx$ where FECHA_CXR like '%06/2017%'
delete cxr_rnx$ where FECHA_CXR like '%06/2017%'




Use Movistar_104_v2
select * from Agent_Queue_Report_30min where [Day] like '%2017-08-04%'
select * from [Agent_Summary_Activity_Report_Active_30min] where [Day] like '%2017-07-04%'
delete [Agent_Summary_Activity_Report_Active_30min] where [Day] like '%2016-11%' 
delete [Agent_Summary_Activity_Report_Interaction_30min] where [Day] like '%2016-11%'
select * from [Agent_Summary_Activity_Report_Interaction_30min] where [Day] like '%2016-11-23%'
delete [Agent_Summary_Activity_Report_Interaction_30min] where [Day] like '%2016-11-24%'


select * from dbo.[CORTEPORROBO-OUT] where YEAR([Fecha y Hora])='2017' and MONTH([Fecha y Hora])='07'


select * from T_V_Maestrohistorico104  where YEAR(fecha)='2017' AND MONTH(FECHA)in('05') and DAY(FECHA)='06'

USE Movistar_104_v2
select * from Agent_Queue_Report_30min where DAY like '%2017-07-%'


select * from V_MaestroHistoricoSuperiores104 where YEAR(fecha)='2017' AND MONTH(FECHA)in('08') and DAY(FECHA)='06'

select * from T_V_Maestrohistorico104 where YEAR(fecha)='2017' AND MONTH(FECHA)in('08')

select * from TAB_V_BASE_INTEGRADO_INFOMART where YEAR(fecha)='2017' AND MONTH(FECHA)in('08')





select [Tenant Name] from Interaction_Handling_Attempt where [Handling Attempt Start] >= '20170815' 
delete Interaction_Handling_Attempt where [Tenant Name] = 'End Range'

select * from Queue_Outline_Report where [30 minutes] like '2017-08-15%'

select * from Queue_Summary_Report where [30 minutes] like '2017-08-15%'

select * from Abandon_Delay_Report where [30 minutes] like '2017-08-15%'

select * from [Speed_of_Accept_(seconds)_Report] where [30 minutes] like '2017-08-15%'

select * from Queue_Outline_Report where [30 minutes] like '2017-08-15%'

select [Interaction Type] from Interaction_Handling_Attempt where [Handling Attempt Start] >= '20170815' and [Interaction Type] is NULL



UPDATE base_bigbang_residencial201708 set [Estado 4] = 'Plan Ahorro Elige + S/ 25.00 '
where [Estado 4] = 'Plan Ahorro Elige + S/ 25' and [Fecha y Hora] > = '20170901'

UPDATE base_bigbang_residencial201708 set [Estado 4] = 'Plan Ahorro Elige + S/ 29.00'
where [Estado 4] = 'Plan Ahorro Elige + S/ 29' and [Fecha y Hora] > = '20170901'

UPDATE base_bigbang_residencial201708 set [Estado 4] = 'Plan Ahorro Elige + S/ 35.00'
where [Estado 4] = 'Plan Ahorro Elige + S/ 35' and [Fecha y Hora] > = '20170901'

UPDATE base_bigbang_residencial201708 set [Estado 4] = 'Plan Ahorro Elige + S/ 39.00'
where [Estado 4] = 'Plan Ahorro Elige + S/ 39'

UPDATE base_bigbang_residencial201708 set [Estado 4] = 'Plan Ahorro Elige + S/ 45.00'
where [Estado 4] = 'Plan Ahorro Elige + S/ 45'





UPDATE RESIDENCIALAGOSTO2017 set [Estado 4] = 'Plan Ahorro Elige + S/ 25.00 '
where [Estado 4] = 'Plan Ahorro Elige + S/ 25' and [Fecha y Hora] > = '20170801'

UPDATE RESIDENCIALAGOSTO2017 set [Estado 4] = 'Plan Ahorro Elige + S/ 29.00'
where [Estado 4] = 'Plan Ahorro Elige + S/ 29' and [Fecha y Hora] > = '20170801'

UPDATE RESIDENCIALAGOSTO2017 set [Estado 4] = 'Plan Ahorro Elige + S/ 35.00'
where [Estado 4] = 'Plan Ahorro Elige + S/ 35' and [Fecha y Hora] > = '20170801'

UPDATE RESIDENCIALAGOSTO2017 set [Estado 4] = 'Plan Ahorro Elige + S/ 39.00'
where [Estado 4] = 'Plan Ahorro Elige + S/ 39' and [Fecha y Hora] > = '20170801'

UPDATE RESIDENCIALAGOSTO2017 set [Estado 4] = 'Plan Ahorro Elige + S/ 45.00'
where [Estado 4] = 'Plan Ahorro Elige + S/ 45' and [Fecha y Hora] > = '20170801'
