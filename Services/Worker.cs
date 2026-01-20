using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Data.SqlClient;
using System.Linq;


public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _config;
    private readonly SapService _sap;

    public Worker(
        ILogger<Worker> logger,
        IConfiguration config,
        SapService sap
    )
    {
        _logger = logger;
        _config = config;
        _sap = sap;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Console.Title = "Sistema de Inventarios | Integración SAP";

        _logger.LogInformation("==============================================");
        _logger.LogInformation(" Sistema de Inventarios");
        _logger.LogInformation(" Módulo : Integración SAP");
        _logger.LogInformation(" Estado : Iniciado");
        _logger.LogInformation("==============================================");

        using var conn = new SqlConnection(_config.GetConnectionString("SqlServer"));
        await conn.OpenAsync(stoppingToken);

        // ================================
        // CONECTAR SAP (UNA SOLA VEZ)
        // ================================
        _logger.LogInformation("Conectando a SAP...");
        _sap.Connect(
            _config["SapDiApi:Server"],
            _config["SapDiApi:CompanyDb"],
            _config["SapDiApi:User"],
            _config["SapDiApi:Password"],
            _config["SapDiApi:LicenseServer"],
            _config["SapDiApi:DbUser"],
            _config["SapDiApi:DbPassword"],
            int.Parse(_config["SapDiApi:DbServerType"]!)
        );
        _logger.LogInformation("SAP conectado correctamente");

        // ================================
        // LOOP PERMANENTE
        // ================================
        while (!stoppingToken.IsCancellationRequested)
        {
            var señales = new List<(int IdSignal, int IdCierre)>();

            // =========================================
            // 1) OBTENER SEÑALES PENDIENTES + ESTATUS=3
            // =========================================
            using (var cmd = new SqlCommand(@"
                SELECT s.id_signal,
                       s.id_cierre
                FROM CAP_SAP_SIGNAL s
                INNER JOIN CAP_INVENTARIO_CIERRE c
                    ON c.id_cierre = s.id_cierre
                WHERE s.procesado = 0
                  AND c.estatus_cierre  = 3
                ORDER BY s.fecha_signal
            ", conn))
            using (var rd = await cmd.ExecuteReaderAsync(stoppingToken))
            {
                while (await rd.ReadAsync(stoppingToken))
                {
                    señales.Add((
                        rd.GetInt32(0),
                        rd.GetInt32(1)
                    ));
                }
            }

            if (señales.Count == 0)
            {
                _logger.LogInformation("Sistema en espera | Sin señales válidas");
                await Task.Delay(5000, stoppingToken);
                continue;
            }

            // ================================
            // 2) PROCESAR CADA SEÑAL
            // ================================
            foreach (var s in señales)
            {
                int idSignal = s.IdSignal;
                int idCierre = s.IdCierre;

                _logger.LogInformation("----------------------------------------------");
                _logger.LogInformation("Señal detectada | id_signal={Signal} | id_cierre={Cierre}",
                    idSignal, idCierre);

                // =====================================
                // 2.1 BLOQUEAR CIERRE (evitar duplicado)
                // =====================================
                using (var lockCmd = new SqlCommand(@"
                    UPDATE CAP_INVENTARIO_CIERRE
                    SET estatus_cierre  = 4
                    WHERE id_cierre = @id
                      AND estatus_cierre  = 3
                ", conn))
                {
                    lockCmd.Parameters.AddWithValue("@id", idCierre);
                    int rows = await lockCmd.ExecuteNonQueryAsync(stoppingToken);

                    if (rows == 0)
                    {
                        _logger.LogWarning("Cierre {Cierre} omitido | Estatus ya no es 3", idCierre);
                        continue;
                    }
                }

                int ok = 0;
                int err = 0;

                try
                {
                    // ================================
                    // 2.2 CONFIG CONTABLE
                    // ================================
                    string proyecto;
                    string cuentaEM;
                    string cuentaSM;
                    DateTime fechaInventario;

                    using (var cmdCfg = new SqlCommand(@"
                        SELECT proyecto, cuenta_em, cuenta_sm, fecha_inventario
                        FROM CAP_INVENTARIO_CIERRE_CONFIG
                        WHERE id_cierre = @id
                    ", conn))
                    {
                        cmdCfg.Parameters.AddWithValue("@id", idCierre);
                        using var rdCfg = await cmdCfg.ExecuteReaderAsync(stoppingToken);

                        if (!await rdCfg.ReadAsync(stoppingToken))
                            throw new Exception("No existe configuración contable");

                        proyecto = rdCfg.GetString(0);
                        cuentaEM = rdCfg.GetString(1);
                        cuentaSM = rdCfg.GetString(2);
                        fechaInventario = rdCfg.GetDateTime(3);
                    }

                    // ================================
                    // 2.3 AJUSTES PENDIENTES
                    // ================================
                    var ajustes = new List<(int Id, string Item, decimal Qty, string Tipo, string Almacen, string Comentarios)>();

                    using (var cmdAjustes = new SqlCommand(@"
                        SELECT id_ajuste,
                               ItemCode,
                               ABS(cantidad_ajuste),
                               tipo_ajuste,
                               almacen,
                               comentarios
                        FROM CAP_INVENTARIO_AJUSTES_SAP
                        WHERE id_cierre = @id
                          AND estado_proceso = 1
                        ORDER BY id_ajuste
                    ", conn))
                    {
                        cmdAjustes.Parameters.AddWithValue("@id", idCierre);
                        using var reader = await cmdAjustes.ExecuteReaderAsync(stoppingToken);

                        while (await reader.ReadAsync(stoppingToken))
                        {
                            ajustes.Add((
                                reader.GetInt32(0),
                                reader.GetString(1),
                                reader.GetDecimal(2),
                                reader.GetString(3),
                                reader.GetString(4),
                                reader.IsDBNull(5) ? "" : reader.GetString(5)
                            ));
                        }
                    }

                    // ================================
                    // 2.4 PROCESAR AJUSTES
                    // ================================
                    static string GetBaseLocal(string alm)
                    {
                        if (string.IsNullOrWhiteSpace(alm)) return "";
                        alm = alm.Trim();
                        int p = alm.IndexOf('-');
                        return p > 0 ? alm.Substring(0, p) : alm;
                    }

                    var gruposAjustes = ajustes
                        .GroupBy(a => new { Base = GetBaseLocal(a.Almacen), Tipo = a.Tipo })
                        .OrderBy(g => g.Key.Base)
                        .ThenBy(g => g.Key.Tipo);

                    foreach (var g in gruposAjustes)
                    {
                        var baseLocal = g.Key.Base; // CJN, AAA, etc
                        var tipo = g.Key.Tipo;      // "E" o "S"

                        try
                        {
                            string cuenta = tipo == "E" ? cuentaEM : cuentaSM;

                            // ✅ Separar inventariables / no inventariables
                            var inventariables = g
                                .Where(a => _sap.EsArticuloInventario(a.Item))
                                .ToList();

                            var noInventariables = g
                                .Where(a => !_sap.EsArticuloInventario(a.Item))
                                .ToList();

                            if (inventariables.Count == 0)
                                throw new Exception("No hay artículos inventariables para generar documento SAP");

                            // Líneas válidas para SAP
                            var lines = inventariables.Select(a => (
                                ItemCode: a.Item,
                                QtyAbs: a.Qty,
                                Warehouse: a.Almacen,
                                AccountCode: cuenta,
                                ProjectCode: proyecto
                            )).ToList();

                            string comments = g
                            .Select(x => x.Comentarios)
                            .FirstOrDefault(c => !string.IsNullOrWhiteSpace(c));

                            if (comments == null)
                                comments = "";


                            var (docEntry, docNum) =
                                _sap.CreateInventoryAdjustmentBatch(
                                    tipo,
                                    lines,
                                    comments,
                                    fechaInventario
                                );

                            // UPDATE OK para inventariables (mismo DocEntry/DocNum)
                            var idsOk = inventariables.Select(x => x.Id).ToList();
                            var inParamsOk = idsOk.Select((_, idx) => $"@id{idx}").ToArray();

                            string sqlOk = $@"
UPDATE CAP_INVENTARIO_AJUSTES_SAP
SET estado_proceso = 2,
    tipo_documento_sap = @tipoDoc,
    DocEntry_sap = @docEntry,
    DocNum_sap = @docNum,
    fecha_procesado = GETDATE(),
    usuario_procesado = 'SICAFSAP'
WHERE id_ajuste IN ({string.Join(",", inParamsOk)})
";

                            using (var okCmd = new SqlCommand(sqlOk, conn))
                            {
                                okCmd.Parameters.AddWithValue("@tipoDoc", tipo == "E" ? "OIGN" : "OIGE");
                                okCmd.Parameters.AddWithValue("@docEntry", docEntry);
                                okCmd.Parameters.AddWithValue("@docNum", docNum);

                                for (int i = 0; i < idsOk.Count; i++)
                                    okCmd.Parameters.AddWithValue($"@id{i}", idsOk[i]);

                                await okCmd.ExecuteNonQueryAsync(stoppingToken);
                            }

                            ok += idsOk.Count;

                            // UPDATE ERROR para NO inventariables
                            if (noInventariables.Count > 0)
                            {
                                var idsNI = noInventariables.Select(x => x.Id).ToList();
                                var inParamsNI = idsNI.Select((_, idx) => $"@nid{idx}").ToArray();

                                string sqlNI = $@"
UPDATE CAP_INVENTARIO_AJUSTES_SAP
SET estado_proceso = 9,
    mensaje_error_sap = 'Artículo no inventariable',
    fecha_procesado = GETDATE(),
    usuario_procesado = 'SICAFSAP'
WHERE id_ajuste IN ({string.Join(",", inParamsNI)})
";

                                using (var niCmd = new SqlCommand(sqlNI, conn))
                                {
                                    for (int i = 0; i < idsNI.Count; i++)
                                        niCmd.Parameters.AddWithValue($"@nid{i}", idsNI[i]);

                                    await niCmd.ExecuteNonQueryAsync(stoppingToken);
                                }

                                err += idsNI.Count;
                            }
                        }
                        catch (Exception ex)
                        {
                            var ids = g.Select(x => x.Id).ToList();
                            var inParams = ids.Select((_, idx) => $"@id{idx}").ToArray();

                            string sqlErr = $@"
UPDATE CAP_INVENTARIO_AJUSTES_SAP
SET estado_proceso = 9,
    mensaje_error_sap = @error,
    intentos_envio = ISNULL(intentos_envio,0) + 1,
    fecha_ultimo_intento = GETDATE(),
    fecha_procesado = GETDATE(),
    usuario_procesado = 'SICAFSAP'
WHERE id_ajuste IN ({string.Join(",", inParams)})
";

                            using (var errCmd = new SqlCommand(sqlErr, conn))
                            {
                                errCmd.Parameters.AddWithValue("@error", ex.Message);

                                for (int i = 0; i < ids.Count; i++)
                                    errCmd.Parameters.AddWithValue($"@id{i}", ids[i]);

                                await errCmd.ExecuteNonQueryAsync(stoppingToken);
                            }

                            err += ids.Count;
                        }
                    }


                    // ================================
                    // 2.5 CIERRE OK
                    // ================================
                    using var finCmd = new SqlCommand(@"
                        UPDATE CAP_INVENTARIO_CIERRE
                        SET estatus_cierre  = 5
                        WHERE id_cierre = @id
                    ", conn);

                    finCmd.Parameters.AddWithValue("@id", idCierre);
                    await finCmd.ExecuteNonQueryAsync(stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error procesando cierre {Cierre}", idCierre);

                    using var errClose = new SqlCommand(@"
                        UPDATE CAP_INVENTARIO_CIERRE
                        SET estatus_cierre  = 9
                        WHERE id_cierre = @id
                    ", conn);

                    errClose.Parameters.AddWithValue("@id", idCierre);
                    await errClose.ExecuteNonQueryAsync(stoppingToken);
                }
                finally
                {
                    // ================================
                    // 2.6 MARCAR SEÑAL PROCESADA
                    // ================================
                    using var updSignal = new SqlCommand(@"
                        UPDATE CAP_SAP_SIGNAL
                        SET procesado = 1,
                            fecha_proc = GETDATE(),
                            worker_host = HOST_NAME()
                        WHERE id_signal = @id
                    ", conn);

                    updSignal.Parameters.AddWithValue("@id", idSignal);
                    await updSignal.ExecuteNonQueryAsync(stoppingToken);

                    _logger.LogInformation(
                        "Cierre {Cierre} FINALIZADO | OK={Ok} | ERROR={Err}",
                        idCierre, ok, err
                    );
                }
            }
        }
    }
}
