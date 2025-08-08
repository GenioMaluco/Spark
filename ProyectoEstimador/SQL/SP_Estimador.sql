USE [DatosDavivienda]
GO
/****** Object:  UserDefinedFunction [dbo].[fncObtenerCampoHistorico]    Script Date: 8/7/2025 4:26:06 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER FUNCTION [dbo].[fncObtenerCampoHistorico]
(	
	@Cedula varchar(20), @FuenteReferencia int
)
RETURNS @Salida table(Id int identity(1,1),CedulaReferencia varchar(20),Historico varchar(MAX),HistoricoMes varchar(200))
AS
Begin
	
	Declare @Proceso table(Id int Identity(1,1),FechaInformacion date,DiasMora int, Calificacion int,Mes varchar(20))
	Declare @MesInicioFinal table (Fecha date)
	insert into @Proceso(FechaInformacion,DiasMora)
	Select fecha_informacion,dias_mora from ReferenciasComerciales.dbo.DatoReferencia
	where Identificacion = @Cedula and fuente_informacion_id = @FuenteReferencia
	
	delete @Proceso where DiasMora < 1
	Delete @Proceso where FechaInformacion < dateadd(year,-4,getdate())

	Declare @Inicio date = (Select top 1 FechaInformacion from @Proceso order by FechaInformacion asc)
	Declare @Final date = (Select top 1 FechaInformacion from @Proceso order by FechaInformacion desc)
	set @Inicio = dateadd(month,-23,@Final)

	--declare @FechaIteracion date = @Inicio
	--while(convert(int,convert(varchar(6),@FechaIteracion,112)) <= convert(int,convert(varchar(6),@Final,112)))
	--begin
	--	insert into @MesInicioFinal(Fecha) values(@FechaIteracion)
	--	Set @FechaIteracion = DATEADD(month,1,@FechaIteracion)
	--End

	insert into @MesInicioFinal(Fecha)
	select Fecha from Credid_Functions.CALENDARIO.Meses where Fecha between @Inicio and @Final
	order by Fecha


	update @Proceso set DiasMora = t.DiasMoraTotal
	from @Proceso p inner join 
	(
		Select convert(varchar(6),FechaInformacion,112) FechaInformacion,Sum(DiasMora) DiasMoraTotal from @Proceso
		group by convert(varchar(6),FechaInformacion,112)
	) t on convert(varchar(6),p.FechaInformacion,112) = t.FechaInformacion

	;WITH CTE AS(
	   SELECT Id, Cantidad = ROW_NUMBER() OVER(PARTITION BY convert(varchar(6),FechaInformacion,112) ORDER BY Id)
	   FROM @Proceso
	)
	delete FROM CTE WHERE Cantidad > 1

	update @Proceso set Calificacion = 
	case 
		when DiasMora between 1 and 30 then 1
		when DiasMora between 31 and 60 then 2
		when DiasMora between 61 and 90 then 3
		when DiasMora between 91 and 120 then 4
		when DiasMora between 121 and 150 then 5
		when DiasMora > 150 then 6
	end,
	Mes = LEFT(DATENAME(MONTH,FechaInformacion),3)

	Declare @Historico varchar(MAX) = ''
	Declare @HistoricoMes varchar(200) = ''	

	select @Historico = @Historico + ' ' + isnull(CONVERT(varchar(30),Calificacion),'X')
	from @Proceso p full outer join @MesInicioFinal m
	on convert(varchar(6),p.FechaInformacion,112) = convert(varchar(6),m.Fecha,112)
	order by m.Fecha asc

	select @HistoricoMes = @HistoricoMes + ' ' + LEFT(DATENAME(MONTH,m.Fecha),3) + '-' + FORMAT(m.Fecha, 'yy')
	from @Proceso p full outer join @MesInicioFinal m
	on convert(varchar(6),p.FechaInformacion,112) = convert(varchar(6),m.Fecha,112)
	order by m.Fecha asc

	set @Historico = replace(ltrim(rtrim(@Historico)),' ','-')
	set @HistoricoMes = replace(ltrim(rtrim(@HistoricoMes)),' ','/')

	insert into @Salida(CedulaReferencia,Historico,HistoricoMes)
	values(@Cedula,@Historico,@HistoricoMes)

	return
End
