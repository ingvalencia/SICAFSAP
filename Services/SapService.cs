using System;
using SAPbobsCOM;
using System.Runtime.InteropServices;

public sealed class SapService : IDisposable
{
    private Company? _company;

    public void Connect(
        string server,
        string companyDb,
        string userName,
        string password,
        string licenseServer,
        string dbUser,
        string dbPassword,
        int dbServerType
    )
    {
        _company = new Company
        {
            Server = server,
            CompanyDB = companyDb,
            UserName = userName,
            Password = password,
            LicenseServer = licenseServer,
            DbUserName = dbUser,
            DbPassword = dbPassword,
            DbServerType = (BoDataServerTypes)dbServerType,
            language = BoSuppLangs.ln_Spanish_La
        };

        int rc = _company.Connect();
        if (rc != 0)
        {
            _company.GetLastError(out int err, out string msg);
            throw new Exception($"SAP Connect error {err}: {msg}");
        }
    }

    public bool EsArticuloInventario(string itemCode)
    {
        if (_company == null)
            return false;

        var rs = (Recordset)_company.GetBusinessObject(BoObjectTypes.BoRecordset);

        rs.DoQuery($@"
            SELECT InvntItem
            FROM OITM
            WHERE ItemCode = '{itemCode}'
        ");

        bool esInventario =
            rs.RecordCount > 0 &&
            rs.Fields.Item(0).Value.ToString() == "Y";

        Marshal.ReleaseComObject(rs);
        return esInventario;
    }

    public (int DocEntry, int DocNum) CreateInventoryAdjustment(
     string tipoAjuste,
     string itemCode,
     decimal quantityAbs,
     string warehouse,
     string accountCode,
     string projectCode,
     string comments,
     DateTime fechaDocumento
 )
    {
        if (_company == null || !_company.Connected)
            throw new Exception("SAP no conectado");

        BoObjectTypes objType =
            tipoAjuste == "E"
                ? BoObjectTypes.oInventoryGenEntry
                : BoObjectTypes.oInventoryGenExit;

        Documents doc = (Documents)_company.GetBusinessObject(objType);

        // 
        doc.DocDate = fechaDocumento;
        doc.TaxDate = fechaDocumento;

        // 
        doc.Reference2 = "AJUSTEINV";

        // 
        doc.Comments = comments;

        doc.Lines.ItemCode = itemCode;
        doc.Lines.WarehouseCode = warehouse;
        doc.Lines.Quantity = (double)quantityAbs;
        doc.Lines.AccountCode = accountCode;
        doc.Lines.ProjectCode = projectCode;
        doc.Lines.Add();

        int rc = doc.Add();
        if (rc != 0)
        {
            _company.GetLastError(out int err, out string msg);
            Marshal.ReleaseComObject(doc);
            throw new Exception($"SAP Add error {err}: {msg}");
        }

        int docEntry = int.Parse(_company.GetNewObjectKey());

        Recordset rs = (Recordset)_company.GetBusinessObject(BoObjectTypes.BoRecordset);
        string tabla = tipoAjuste == "E" ? "OIGN" : "OIGE";
        rs.DoQuery($"SELECT DocNum FROM {tabla} WHERE DocEntry = {docEntry}");

        int docNum = Convert.ToInt32(rs.Fields.Item(0).Value);

        Marshal.ReleaseComObject(rs);
        Marshal.ReleaseComObject(doc);

        return (docEntry, docNum);
    }

    public (int DocEntry, int DocNum) CreateInventoryAdjustmentBatch(
    string tipoAjuste,
    List<(string ItemCode, decimal QtyAbs, string Warehouse, string AccountCode, string ProjectCode)> lines,
    string comments,
    DateTime fechaDocumento
)
    {
        if (_company == null || !_company.Connected)
            throw new Exception("SAP no conectado");

        if (lines == null || lines.Count == 0)
            throw new Exception("No hay líneas para el documento SAP");

        BoObjectTypes objType =
            tipoAjuste == "E"
                ? BoObjectTypes.oInventoryGenEntry   // OIGN
                : BoObjectTypes.oInventoryGenExit;   // OIGE

        Documents doc = (Documents)_company.GetBusinessObject(objType);

        doc.DocDate = fechaDocumento;
        doc.TaxDate = fechaDocumento;
        doc.Reference2 = "AJUSTEINV";
        doc.Comments = comments;

        bool first = true;

        foreach (var ln in lines)
        {
            if (!first)
                doc.Lines.Add();

            doc.Lines.ItemCode = ln.ItemCode;
            doc.Lines.WarehouseCode = ln.Warehouse;
            doc.Lines.Quantity = (double)ln.QtyAbs;
            doc.Lines.AccountCode = ln.AccountCode;
            doc.Lines.ProjectCode = ln.ProjectCode;

            first = false;
        }

        int rc = doc.Add();
        if (rc != 0)
        {
            _company.GetLastError(out int err, out string msg);
            Marshal.ReleaseComObject(doc);
            throw new Exception($"SAP Add error {err}: {msg}");
        }

        int docEntry = int.Parse(_company.GetNewObjectKey());

        Recordset rs = (Recordset)_company.GetBusinessObject(BoObjectTypes.BoRecordset);
        string tabla = tipoAjuste == "E" ? "OIGN" : "OIGE";
        rs.DoQuery($"SELECT DocNum FROM {tabla} WHERE DocEntry = {docEntry}");

        int docNum = Convert.ToInt32(rs.Fields.Item(0).Value);

        Marshal.ReleaseComObject(rs);
        Marshal.ReleaseComObject(doc);

        return (docEntry, docNum);
    }




    public void Dispose()
    {
        try
        {
            if (_company != null && _company.Connected)
            {
                _company.Disconnect();
                Marshal.ReleaseComObject(_company);
            }
        }
        catch { }
    }
}
