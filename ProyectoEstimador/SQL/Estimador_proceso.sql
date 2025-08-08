USE [DatosDavivienda]
GO
/****** Object:  StoredProcedure [dbo].[spImportReferencias_BackUp]    Script Date: 8/7/2025 5:00:52 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
ALTER PROCEDURE [dbo].[spImportReferencias_BackUp]
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	
	Declare @Inicio datetime = getdate()

	Declare @FechaTope date = dateadd(MONTH,-24,getdate())
	Declare @ErrorInsert bit = 0
	Declare @ErrorCampoHistorico bit = 0

	--insert into ReferenciasComerciales.dbo.ReferenciasDavivienda(IdReferencia,FuenteInformacion,DiasMora,Estado)
	--select Id,fuente_informacion_id,dias_mora,Estado 
	--from ReferenciasComerciales.dbo.DatoReferencia d 
	--where 
	--fecha_informacion > @FechaTope
	
	--set rowcount 0
	--while(1=1)
	--Begin
	--	delete ReferenciasComerciales.dbo.DatosRerenciasImport where fuente_informacion_id not in(1,25)--Hacienda,ViajesColon

	--	if(@@ROWCOUNT = 0)
	--	Begin
	--		Break
	--	End
	--end


	--delete ReferenciasComerciales.dbo.DatosRerenciasImport where dias_mora > 0
	--delete ReferenciasComerciales.dbo.DatosRerenciasImport where estado = 1

	--delete ReferenciasComerciales.dbo.DatoReferencia where fecha_informacion < @FechaTope
	--fecha_informacion >= @FechaTope and

	truncate table DatosDavivienda.dbo.CLI_REFERENCIASCREDITICIAS_BackUp
	DBCC CHECKIDENT ('DatosDavivienda.dbo.CLI_REFERENCIASCREDITICIAS_BackUp',RESEED,0)
	
	insert into DatosDavivienda.dbo.CLI_REFERENCIASCREDITICIAS_BackUp
	(
		Num_Referencia,NumCedula_Cliente,Entidad,TipoOperacion,EstadoOperacion,TipoDeudor,TipoGarantia
		,SectorCredito,FechaOtorgado,FechaVencimiento,Principal,SaldoLocalColones,SaldoLocalDolares
		,SaldoMoraColones,SaldoMoraDolares,Cuota,DiasAtraso,CuotasVencidas,Historico,Fec_Actualizacion
		,Responsabilidad,Fecha_Ultimo_Pago,Cat_Sugef_Colones,Dias_Atraso_Dolares,Cat_Sugef_Dolares,Codigo_Unico,Historico_Mes
	)
	select
	d.Id Num_referencia,
	identificacion NumCedula,
	c.Cliente Entidad,
	case tipo_credito_id
		when 1 then 'Tarjeta de Crédito comercial'
		when 2 then 'Crédito a Plazo'
		when 3 then 'Crédito Rotativo'
		when 4 then 'Tarjeta de crédito internacional'
		when 5 then 'Tarjeta de Crédito local'
		when 6 then 'Hipotecario'
		when 7 then 'Efectivo a 30 días Plazo'
		when 8 then 'Prendario'
		when 9 then 'Refinamiento'
		when 10 then 'Otros'
		when 11 then 'Pago bienes inmuebles'
		when 12 then 'Pago patentes'
		when 13 then 'LineaBlanca'
		when 14 then 'Prestamo Comercial'
		when 15 then 'Empresarial'
		when 16 then 'Cuota nivelada'
		when 17 then 'Linea Crédito'
		when 18 then 'Fiduciario'
		when 19 then 'Automovil Prendario'
		when 20 then 'Capital Social'
		when 21 then 'CPH-3 Fiduciario'
		when 22 then 'CPH-3 Hipotecario'
		when 23 then 'CPH-3 Sin Fiador'
		when 24 then 'Hipotecario Consumo'
		when 25 then 'Hipotecario Cuota Tradicional (Cerrado)'
		when 26 then 'MULTIPLUSCRÉDI'
		when 27 then 'Premium'
		when 28 then 'Sin Fiador'
		when 29 then 'Uso Multiple'
		when 30 then 'Vivienda'
	End,
	--ct.Descripcion TipoOperacion,
	case codigo_estado_cuenta_id
		when  1 then 'Cuenta Nueva'
		when  2 then 'Cuenta cancelada por el cliente'
		when  3 then 'Cuenta cancelada por el Acreedor'
		when  4 then 'Cuenta en Mora'
		when  5 then 'Cuenta en Cobro judicial'
		when  6 then 'Cuenta Incobrable'
		when  7 then 'Cuenta con arreglo de pago'
		when  8 then 'Cuenta en Cobro Administrativo'
		when  9 then 'Cuenta en estudio'
		when  10 then 'Cuenta al día'
		when  11 then 'Cancelado con atraso'
		when  12 then 'Cobro especial'
		when  13 then 'Cartera Separada'
		when  14 then 'Cancelado'
		when  15 then 'Excluída'
		when  16 then 'No indica'
		when  17 then 'Deuda declarada prescrita o incobrable por el juzgado de cobro de Heredia el 26 de setiembre del 2017'
		when  18 then 'Cuenta con Problemas'
		when  19 then 'Cuenta de Baja'
		when  20 then 'Cuenta Normal'
		when  21 then 'Cuenta en Pre-Cobro Judicial'
		when  22 then 'Cuenta Vencida'
		when  23 then 'Cobro Pre-Legal'
		when  24 then 'Cancelado En Arreglo Extrajudicial'
		when  25 then 'Traslado a Cobro'
		when  26 then 'Crédito cerrado'
		when  27 then 'Cobro Legal'
		when  28 then 'Cobro Ordinario'
		when  29 then 'Rechazado'
		when  30 then 'Aplicación Parcial'
		when  31 then 'Activo'
		when  32 then 'Reestructuracion'
		when  33 then 'Desembolsado'
		else 'Cuenta en Mora'
	end EstadoOperacion,
	--isnull(et.Descripcion,'Cuenta en Mora') EstadoOperacion,
	--dt.Descripcion TipoDeudor,
	case tipo_deudor_id
		when 1 then 'Principal'
		when 2 then 'Codeudor'
		when 3 then 'Fiador'
		when 4 then 'Fideicomitente'
	End TipoDeudor,
	'' TipoGarantia,
	--it.Descripcion SectorCredito,
	case tipo_informacion_id
		when 1 then 'Banco Estatal'
		when 2 then 'Banca Privada'
		when 3 then 'Financiera Regulada'
		when 4 then 'Financiera No Regulada'
		when 5 then 'Cooperativa'
		when 6 then 'Mutual'
		when 7 then 'Empresa Administradora de Tarjetas de Crédito'
		when 8 then 'Venta de Catalogos'
		when 9 then 'Distribuidora de Electródomesticos'
		when 10 then 'Industria'
		when 11 then 'Construcción'
		when 12 then 'Telecomunicaciones'
		when 13 then 'Servicios Públicos'
		when 14 then 'IMF'
		when 15 then 'Agencias de Viaje'
		when 16 then 'Distribuidoras de Vehículos'
		when 17 then 'Comercio'
		when 18 then 'Otros'
		when 19 then 'Colegios Profesionales'
		when 20 then 'Municipalidades'
		when 21 then 'Tienda por departamentos'
		when 22 then 'Cooperativas de Ahorro y Crédito'
		when 23 then 'Título personal'
		when 24 then 'Universidades'
		when 25 then 'Asociación Solidarista'
		when 26 then 'Laboratorio Dental'
		when 27 then 'Servicios de seguridad privada'
		when 28 then 'Farmacia'
		when 29 then 'Gobierno'
		when 30 then 'Aseguradora'
	End SectorCredito,
	format(d.fecha_otorgamiento_credito,'dd/MM/yyyy') FechaOtorgado,
	--convert(varchar,d.fecha_otorgamiento_credito,112) FechaOtorgado,
	d.fecha_vencimiento FechaVencimiento,
	d.saldo_mora Principal,
	iif(tipo_moneda_id = 1,d.saldo_mora,null) SaldoLocalColones,
	iif(tipo_moneda_id = 2,d.saldo_mora,null) SaldoLocalDolares,
	iif(tipo_moneda_id = 1,d.saldo_mora,null) SaldoMoraColones,
	iif(tipo_moneda_id = 2,d.saldo_mora,null) SaldoMoraDolares,
	cuotas_vencidas Cuota,
	iif(tipo_moneda_id = 1,dias_mora,null) DiasAtraso,
	cuotas_vencidas CuotasVencidas,
	t.Historico Historico,
	format(fecha_informacion,'dd/MM/yyyy') Fec_Actualizacion,--Convert(varchar,fecha_informacion,112) 
	--dt.Descripcion Responsabilidad,
	case tipo_deudor_id
		when 1 then 'Principal'
		when 2 then 'Codeudor'
		when 3 then 'Fiador'
		when 4 then 'Fideicomitente'
	End,
	format(d.fecha_ultimo_pago,'dd/MM/yyyy')Fecha_Ultimo_Pago,--Convert(varchar,fecha_ultimo_pago,112) Fecha_Ultimo_Pago,
	iif(tipo_moneda_id = 1,
	case 
		when dias_mora <= 90 and codigo_estado_cuenta_id <> 5 then 2
		when dias_mora > 90 and dias_mora <=180 and codigo_estado_cuenta_id <> 5 then 3
		when dias_mora > 180 and codigo_estado_cuenta_id <> 5 then 4
		else iif(codigo_estado_cuenta_id = 5,5,0)
	end,0) Cat_Sugef_Colones,
	iif(tipo_moneda_id = 2,dias_mora,null) Dias_Atraso_Dolares,
	iif(tipo_moneda_id = 2,
	case 
		when dias_mora <= 90 and codigo_estado_cuenta_id <> 5 then 2
		when dias_mora > 90 and dias_mora <=180 and codigo_estado_cuenta_id <> 5 then 3
		when dias_mora > 180 and codigo_estado_cuenta_id <> 5 then 4
		else iif(codigo_estado_cuenta_id = 5,5,0)
	end,0) Cat_Sugef_Dolares,
	d.Id Codigo_Unico,
	t.HistoricoMes Historico_Mes
	from ReferenciasComerciales.dbo.DatoReferencia d 
	inner join ReferenciasComerciales.dbo.ClienteFuente c on d.fuente_informacion_id = c.id and c.VersionDatos = d.fecha_informacion
	--inner join ReferenciasComerciales.dbo.InformacionTipo it on it.id = d.tipo_informacion_id
	--inner join ReferenciasComerciales.dbo.DeudorTipo dt on dt.id = d.tipo_deudor_id
	--inner join ReferenciasComerciales.dbo.CreditoTipo ct on ct.id = d.tipo_credito_id
	--left join ReferenciasComerciales.dbo.EstadoCuentaTipo et on et.id = d.codigo_estado_cuenta_id
	outer apply dbo.fncObtenerCampoHistorico(d.identificacion,d.fuente_informacion_id) t
	where 
	fecha_informacion >= @FechaTope
	and dias_mora > 0
	and d.estado = 1
	--order by newid()
	
	--select * from ReferenciasComerciales.dbo.DatoReferencia where id in(1,25)


	/*
	(1) Vigente, 
	(2) Vencida menor o igual a 90 días, 
	(3) Vencida a más de 90 días y menor o igual a 180 días, 
	(4) Vencida a más de 180 días, 
	(5) Cobro Judicial.
	*/

	Delete CLI_REFERENCIASCREDITICIAS_BackUp where NumCedula_Cliente is null
	Delete CLI_REFERENCIASCREDITICIAS_BackUp where NumCedula_Cliente = ''

	Declare @Final datetime = getdate()
	print 'Tiempo:'
	print datediff(minute,@Inicio,@Final)

	declare @Cantidad int = (Select count(*) from CLI_REFERENCIASCREDITICIAS_BackUp)
	insert into IMP.ControlCantidad(Tabla,Fecha,Cantidad)values('Referencias',getdate(),@Cantidad)

END
