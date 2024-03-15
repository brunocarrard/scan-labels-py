from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix
import datetime
import pyodbc

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
CORS(app)

def get_db_connection():
    server = 'LR-SQL01\MSSQLSERVER_ISAH'  # e.g., 'localhost\sqlexpress'
    database = 'Homologation_Legend_Fleet'
    username = 'IsahIsah'
    password = 'isahisah'

    # Adjust the driver name according to the version installed on your system
    driver_name = 'SQL Server Native Client 11.0'  # Replace with the actual driver name

    cnxn = pyodbc.connect(f'DRIVER={{{driver_name}}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
    return cnxn

@app.route('/')
def data():
    user_input = request.args.get('value', type=str)  # Get user input from query parameter

    if not user_input:
        return jsonify({"error": "Must inform Sales Order"}), 400

    results = get_del_lines(user_input)
    
    if len(results) == 0:
        return jsonify({"error": "Could not find Sales Order"}), 404
    results = work_data(results)

    if len(results["parts"]) == 0:
        return jsonify({"error": "Order is ready and authorized"}), 404

    sub_parts = get_sub_parts(user_input)

    results["parts"] = results["parts"] + sub_parts

    certificates_lookup = get_available_certificates(user_input)

    for part in results["parts"]:
        part_code = part["PartCode"]
        if part_code in certificates_lookup:
            part["available_certificates"] = certificates_lookup[part_code]

    return jsonify(results)

@app.route('/', methods=['POST'])
def handle_post():
    if request.is_json:
        data = request.get_json()
        del_lines = data['delLines']
        del_lines = verify_v1(del_lines)
        lotnr_result = verify_lotnr(del_line["lotNr"] for del_line in del_lines)
        certificate_result = verify_certificate(del_line["certificate"] for del_line in del_lines)
        if not lotnr_result["valid"]:
            lotnrs = ", ".join(lotnr_result["invalid_results"])
            return jsonify({"error": f"LotNr {lotnrs} does not exists"}), 400
        if not certificate_result["valid"]:
            certificates = ", ".join(certificate_result["invalid_results"])
            return jsonify({"error": f"Certiicate {certificates} does not exists"}), 400
        old_del_lines = get_del_lines(data['ordNr'])
        
        for line in old_del_lines:
            line['PartCode'] = line['PartCode'].strip()
        import_del_lines = assembly_del_lines_with_scan_sales_parts([line for line in del_lines if line['SubPartInd'] == 0], old_del_lines)

        create_new_lines(import_del_lines)
        assembly_del_lines_with_scan_production_bom([line for line in del_lines if line['SubPartInd'] == 1], data['ordNr'])
        # create_bom_del_lines([line for line in del_lines if line['SubPartInd'] == 1], data['ordNr'])
        # update_bom_del_lines([line for line in del_lines if line['SubPartInd'] == 1], data['ordNr'])
        authorize(import_del_lines)
        return ("Scans were imported.")
    else:
        return jsonify({"error": "Request must be JSON"}), 400
    

def verify_lotnr(lot_nrs):
    result = {
        "valid": True,
        "invalid_results": []
    }
    cnxn = get_db_connection()
    cursor = cnxn.cursor()
    for lot_nr in lot_nrs:
        cursor.execute("SELECT LotNr FROM T_LotNumber WHERE LotNr = ?", (lot_nr))
        exists = cursor.fetchall()
        if not exists:
            result["invalid_results"].append(lot_nr)
            result["valid"] = False
    cursor.close()
    cnxn.close()
    return result

def verify_certificate(certificates):
    result = {
        "valid": True,
        "invalid_results": []
    }
    cnxn = get_db_connection()
    cursor = cnxn.cursor()
    for certificate in certificates:
        cursor.execute("SELECT CertificateCode FROM T_Certificate WHERE CertificateCode = ?", (certificate))
        exists = cursor.fetchall()
        if not exists:
            result["invalid_results"].append(certificate)
            result["valid"] = False
    cursor.close()
    cnxn.close()
    return result

def get_sub_parts(ord_nr):
    cnxn = get_db_connection()
    cursor = cnxn.cursor()
    cursor.execute("EXEC SIP_sel_LEG_StockMovementsBOM ?", (ord_nr))
    rows = cursor.fetchall()

    columns = [column[0] for column in cursor.description]
    results = []
    for row in rows:
        row_dict = dict(zip(columns, row))

        # Strip whitespace from string values and format date values
        for key, value in row_dict.items():
            if isinstance(value, str):
                # Strip whitespace from string values
                row_dict[key] = value.strip()

        results.append(row_dict)

    cursor.close()
    cnxn.close()
    return results

def get_del_lines(ord_nr):
    cnxn = get_db_connection()
    cursor = cnxn.cursor()
    cursor.execute("EXEC SIP_sel_LEG_StockMovements ?", (ord_nr))
    rows = cursor.fetchall()
    
    # Convert query result to list of dictionaries
    columns = [column[0] for column in cursor.description]
    results = []
    for row in rows:
        row_dict = dict(zip(columns, row))

        # Strip whitespace from string values and format date values
        for key, value in row_dict.items():
            if isinstance(value, str):
                # Strip whitespace from string values
                row_dict[key] = value.strip()

        results.append(row_dict)

    cursor.close()
    cnxn.close()
    return results

def get_available_certificates(ord_nr):
    cnxn = get_db_connection()
    cursor = cnxn.cursor()
    cursor.execute("EXEC SIP_sel_LEG_AvailableCertificates ?", (ord_nr))
    rows = cursor.fetchall()
    
    available_certificates = {}

    for row in rows:
        part_code = row[0]
        provenance = row[1]
        lot_nr = row[2]
        certificate = row[3]
        qty = row[4]

        if int(provenance) == 3:
            if part_code.strip() not in available_certificates:
                available_certificates[part_code.strip()] = []
            
            if not certificate.strip() == "": available_certificates[part_code.strip()].append({"code": certificate.strip(), "qty": int(qty)})
            if not lot_nr.strip() == "": available_certificates[part_code.strip()].append({"code": lot_nr.strip(),  "qty": int(qty)})
    cursor.close()
    cnxn.close()
    cnxn = get_db_connection()
    cursor = cnxn.cursor()
    cursor.execute("EXEC SIP_sel_LEG_ScannedCertificates ?", (ord_nr))
    rows = cursor.fetchall()
    for row in rows:
        part_code = row[0]
        qty = row[1]
        certificate = row[2]
        lot_nr = row[3]
        found_certificate = next((obj for obj in available_certificates[part_code.strip()] if obj["code"] == lot_nr), None)
        if found_certificate:
            found_certificate["qty"] = int(found_certificate["qty"]) - int(qty)
        else:
            found_certificate = next((obj for obj in available_certificates[part_code.strip()] if obj["code"] == certificate), None)
            if found_certificate:
                found_certificate["qty"] = int(found_certificate["qty"]) - int(qty)
    cursor.close()
    cnxn.close()
    return available_certificates

def work_data(result):
    worked_data = {
        "ordNr": result[0]["OrdNr"],
        "parts": []
    }

    grouped_by_part_code = {}

    for item in result:
        part_code = item["PartCode"].strip()
        qty = item["Qty"] - item["ToBeDelQty"] - item["DelQty"]

        if part_code not in grouped_by_part_code:
            grouped_by_part_code[part_code] = qty
        else:
            grouped_by_part_code[part_code] += qty

    worked_data["parts"] = [{"PartCode": key, "Qty": value} for key, value in grouped_by_part_code.items()]

    worked_data["parts"] = [item for item in worked_data["parts"] if item.get("Qty") is not None and item.get("Qty") > 0]

    return worked_data

def verify_v1(del_lines):
    new_del_lines = []
    for line in del_lines:
        line['PartCode'] = line['partCode']
        line['Qty'] = line['qty']
        if (line['lotNr'].startswith("LF") or line['lotNr'].startswith("LEG")):
            line['certificate'] = ""
        else:
            if (line['lotNr']) == "":
                line['certificate'] = ""
            else:
                line['certificate'] = line['lotNr']
            line['lotNr'] = ""
        new_del_lines.append(line)
    return (new_del_lines)

def assembly_del_lines_with_scan_sales_parts(del_lines, old_del_lines):
    # assembly similar del_lines
    sum_dict = {}
    for line in del_lines:
        key = (line['PartCode'], line['lotNr'], line['certificate'])
        sum_dict[key] = sum_dict.get(key, 0) + int(line['Qty'])
    del_lines = [{"PartCode": key[0], "lotNr": key[1], "certificate": key[2], "Qty": qty} for key, qty in sum_dict.items()]
    # set initial import_lines
    import_del_lines = old_del_lines.copy()
    # remove from import_lines lines Parts that where not scanned
    part_codes_set = set(item.get("PartCode") for item in del_lines)
    import_del_lines = [item for item in import_del_lines if item.get("PartCode") in part_codes_set]
    # remove from import_lines lines Parts that where ready or authorized
    import_del_lines = [item for item in import_del_lines if item.get("DelFiatInd") == 0]
    import_del_lines = [item for item in import_del_lines if int(item.get("Qty")) - int(item.get("DelQty")) - int(item.get("ToBeDelQty")) > 0]
    # produce update import_lines
    for import_line in import_del_lines:
        for del_line in del_lines:
            if import_line["PartCode"] == del_line["PartCode"] and import_line.get("Done", False) is not True:
                import_line["ToBeDelQty"] = int(import_line["ToBeDelQty"]) + int(del_line["Qty"])
                import_line["RemainingQty"] = int(import_line["Qty"]) - import_line["ToBeDelQty"]
                import_line["Qty"] = import_line["ToBeDelQty"]
                import_line["ToBeDelCertificateCode"] = del_line["certificate"]
                import_line["ToBeDelLotNr"] = del_line["lotNr"]
                import_line["Done"] = True
                import_line["Authorize"] = True
                del_line["Done"] = True
                if int(import_line["InvtQty"]) > 0:
                    import_line["InvtQty"] = import_line["Qty"]
                if int(import_line["PurQty"]) > 0:
                    import_line["PurQty"] = import_line["Qty"]
                if int(import_line["ProdQty"]) > 0:
                    import_line["ProdQty"] = import_line["Qty"]

    # produce insert import_lines
    for del_line in del_lines:
        if del_line.get("Done", False) is not True:
            for import_line in import_del_lines:
                if del_line["PartCode"] == import_line["PartCode"] and import_line.get("Done", False) is True:
                    new_line = import_line.copy()
            new_line["PartCode"] = del_line["PartCode"]
            new_line["Qty"] = del_line["Qty"]
            new_line["ToBeDelQty"] = del_line["Qty"]
            new_line["ToBeDelCertificateCode"] = del_line["certificate"]
            new_line["ToBeDelLotNr"] = del_line["lotNr"]
            new_line["RemainingQty"] = 0
            new_line["DelQty"] = 0
            new_line["ToBeDelPurQty"] = 0
            new_line["ToBeDelInvtQty"] = 0
            new_line["ToBeDelProdQty"] = 0
            new_line["DesiredDelDate"] = datetime.datetime.now()
            new_line["Authorize"] = True
            new_line["DelLineLineNr"] = None
            if new_line["InvtQty"] > 0:
                new_line["InvtQty"] = del_line["Qty"]
            if int(new_line["PurQty"]) > 0:
                new_line["PurQty"] = del_line["Qty"]
            if int(new_line["ProdQty"]) > 0:
                new_line["ProdQty"] = del_line["Qty"]
            for import_line in import_del_lines:
                if import_line["PartCode"] == new_line["PartCode"] and import_line.get("RemainingQty", 0) > 0:
                    import_line["RemainingQty"] = import_line["RemainingQty"] - int(new_line["Qty"])
            import_del_lines.append(new_line)

    # produce remaining import_lines
    for import_line in import_del_lines:
        if import_line.get("RemainingQty", 0) > 0:
            new_line = import_line.copy()
            new_line["Qty"] = import_line["RemainingQty"]
            new_line["ToBeDelQty"] = 0
            new_line["RemainingQty"] = 0
            if int(new_line["InvtQty"]) > 0:
                new_line["InvtQty"] = import_line["RemainingQty"]
            if int(new_line["PurQty"]) > 0:
                new_line["PurQty"] = import_line["RemainingQty"]
            if int(new_line["ProdQty"]) > 0:
                new_line["ProdQty"] = import_line["RemainingQty"]
            new_line["DelQty"] = 0
            new_line["ToBeDelPurQty"] = 0
            new_line["ToBeDelInvtQty"] = 0
            new_line["ToBeDelProdQty"] = 0
            new_line["CertificateCode"] = ""
            new_line["LotNr"] = ""
            new_line["DesiredDelDate"] = ""
            new_line["DelLineLineNr"] = None
            new_line["Authorize"] = False
            new_line["RemainingLine"] = True
            import_del_lines.append(new_line)

    return(import_del_lines)

def create_new_lines(import_lines):
    cnxn = get_db_connection()
    cursor = cnxn.cursor()
    # for line in import_lines:
    #     for value in line.values():
    #         if value == "":
    #             value = "N''"
    for line in import_lines:
        if int(line['ProdQty']) > 0:
            line["ToBeDelProdQty"] = line["Qty"]
        else:
            line["ToBeDelInvtQty"] = line["Qty"]
        if line['DelLineLineNr'] is not None:
            if line["LastUpdatedOn"] is not None:
                line["LastUpdatedOn"] = line["LastUpdatedOn"].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            if line["PlanDelDate"] is not None:
                line["PlanDelDate"] = line["PlanDelDate"].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            if line["DelDate"] is not None:
                line["DelDate"] = line["DelDate"].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            if line["ConfDelDate"] is not None:
                line["ConfDelDate"] = line["ConfDelDate"].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            if line["InvtCreDate"] is not None:
                line["InvtCreDate"] = line["InvtCreDate"].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            if line["ToBeDelInvtCreDate"] is not None:
                line["ToBeDelInvtCreDate"] = line["ToBeDelInvtCreDate"].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            if line["ToBeDelCompletedDosDetDate"] is not None:
                line["ToBeDelCompletedDosDetDate"] = line["ToBeDelCompletedDosDetDate"].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            if line["CredLimitExceedsDate"] is not None:
                line["CredLimitExceedsDate"] = line["CredLimitExceedsDate"].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            # UPDATE EXISTING LINES
            cursor.execute("EXEC IP_upd_DeliveryLine ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?", (
                    line['DossierCode'],
                    line['DetailCode'],
                    line['DetailSubCode'],
                    line['DelLineLineNr'],
                    line["LastUpdatedOn"],
                    line['DelMainCode'],
                    line['CustId'],
                    line['DelAddrCode'],
                    line['ShipAgentCode'],
                    line["UserCode"],
                    line["Remark"],
                    line["Qty"],
                    line["PurQty"],
                    line["InvtQty"],
                    line["ProdQty"],
                    line["ToBeDelQty"],
                    line["ToBeDelPurQty"],
                    line["ToBeDelInvtQty"],
                    line["ToBeDelProdQty"],
                    line["DelQty"],
                    line["DelPurQty"],
                    line["DelInvtQty"],
                    line["DelProdQty"],
                    line["PlanDelDate"],
                    datetime.datetime.now(),
                    line["ConfDelDate"],
                    line["DelCompletedDate"],
                    line["DelCompletedInd"],
                    line["Info"],
                    line["WarehouseCode"],
                    line["LocationCode"],
                    line["LotNr"],
                    line["CertificateCode"],
                    '',
                    '',
                    line["ToBeDelLotNr"],
                    line["ToBeDelCertificateCode"],
                    line["InventoryStatusCode"],
                    line["ToBeDelInventoryStatusCode"],
                    line["InvtCreDate"],
                    line["ToBeDelInvtCreDate"],
                    line["ToBeDelCompletedDosDetDate"],
                    line["ToBeDelCompletedDosDetInd"],
                    line["ConfDelDateDefInd"],
                    line["PlanDelDateDefInd"],
                    line["DelAddrCodeDefInd"],
                    line["CredLimitExceedsInd"],
                    line["CredLimitExceedsDate"],
                    line["CredLimitCheckInd"],
                    line["AutoCreShipDocInd"],
                    line["DelAddrType"],
                    0,
                    line["ServObjectCode"],
                    line["TargetServObjectCode"],
                    line["ReplacedABSLineNr"],
                    line["MultiLevelReplacementInd"],
                    line["LocationServObjectCode"],
                    line["MemoGrpId"],
                    1240000,
                    None,
                    'ISAH' 
            ))
            cnxn.commit()
        else:
            # CREATE NEW LINES
            new_DelLineLineNr = None
            LastUpdated = None
            # cursor.execute("")
            params = [
                line.get("DossierCode"),
                line.get("DetailCode"),
                line.get("DetailSubCode"),
                line.get("DelMainCode"),
                line.get("CustId"),
                line.get("DelAddrCode"),
                line.get("ShipAgentCode"),
                line.get("UserCode"),
                line.get("Remark"),
                line.get("Qty"),
                line.get("PurQty"),
                line.get("InvtQty"),
                line.get("ProdQty"),
                line.get("ToBeDelQty"),
                line.get("ToBeDelPurQty"),
                line.get("ToBeDelInvtQty"),
                line.get("ToBeDelProdQty"),
                line.get("DelQty"),
                line.get("DelPurQty"),
                line.get("DelInvtQty"),
                line.get("DelProdQty"),
                line.get("PlanDelDate"),
                line.get("DesiredDelDate"),
                line.get("ConfDelDate"),
                line.get("DelCompletedDate"),
                line.get("DelCompletedInd"),
                line.get("Info"),
                line.get("WarehouseCode"),
                line.get("LocationCode"),
                line.get("LotNr"),
                line.get("CertificateCode"),
                line.get("InventoryStatusCode"),
                line.get("InvtCreDate"),
                line.get("ToBeDelCompletedDosDetInd"),
                line.get("ToBeDelCompletedDosDetDate"),
                line.get("PlanDelDateDefInd"),
                line.get("ConfDelDateDefInd"),
                line.get("DelAddrCodeDefInd"),
                line.get("CredLimitExceedsInd"),
                line.get("CredLimitExceedsDate"),
                line.get("AutoCreShipDocInd"),
                line.get("CredLimitCheckInd"),
                line.get("DelAddrType"),
                0,
                line.get("ServObjectCode"),
                line.get("TargetServObjectCode"),
                line.get("ReplacedABSLineNr"),
                line.get("MultiLevelReplacementInd"),
                1240000,
                'ISAH',
                line.get("LocationServObjectCode"),
                line.get("MemoGrpId") 
            ]
            cursor.execute("DECLARE @new_DelLineLineNr T_LineNr, @LastUpdatedOn nvarchar(30) EXEC IP_ins_DeliveryLine ?, ?, ?, @new_DelLineLineNr OUTPUT, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, @LastUpdatedOn OUTPUT, ?, ?, ? SELECT @new_DelLineLineNr, @LastUpdatedOn", params)
            new_DelLineLineNr = cursor.fetchone()[0]
            cnxn.commit()
            line['DelLineLineNr'] = new_DelLineLineNr
            line["LastUpdatedOn"] = LastUpdated
            line['IsCreated'] = True
    cursor.close()
    cnxn.close()
    update_created_lines(import_lines)

def update_created_lines(import_lines):
    cnxn = get_db_connection()
    cursor = cnxn.cursor()
    for line in import_lines:
        if line.get('IsCreated', False) is True and line.get("RemainingLine", False) is not True:
            cursor.execute("UPDATE T_DeliveryLine SET ToBeDelCertificateCode = ?, ToBeDelLotNr = ? WHERE DossierCode = ? AND DetailCode = ? AND DetailSubCode = ? AND DelLineLineNr = ?", [
                line["ToBeDelCertificateCode"],
                line['ToBeDelLotNr'],
                line['DossierCode'],
                line['DetailCode'],
                line['DetailSubCode'],
                line['DelLineLineNr']
            ])
            cnxn.commit()
    cursor.close()
    cnxn.close()

def authorize(import_lines):
    cnxn = get_db_connection()
    cursor = cnxn.cursor()
    for line in import_lines:
        if line["Authorize"] is not False:
            cursor.execute("DECLARE @InitLogDate T_DateTime, @ReturnCode tinyint, @DeliveryLinesProcessed int EXEC [IP_prc_DeliveryToShip] ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, @InitLogDate OUTPUT, @ReturnCode OUTPUT, @DeliveryLinesProcessed OUTPUT", (
                    line['DossierCode'],
                    line['DetailCode'],
                    line['DetailSubCode'],
                    line['DelLineLineNr'],
                    datetime.datetime.now().strftime("%m/%d/%Y"),
                    0,
                    None,
                    None,
                    'ISAH',
                    1240000
                ))
            cnxn.commit()
    cursor.close()
    cnxn.close()

def assembly_del_lines_with_scan_production_bom(del_lines, ord_nr):
    sum_dict = {}
    for line in del_lines:
        key = (line['PartCode'], line['lotNr'], line['certificate'])
        sum_dict[key] = sum_dict.get(key, 0) + int(line['Qty'])
    del_lines = [{"PartCode": key[0], "lotNr": key[1], "certificate": key[2], "Qty": qty} for key, qty in sum_dict.items()]
    cnxn = get_db_connection()
    cursor = cnxn.cursor()
    for line in del_lines:
        cursor.execute("SIP_ins_LEG_PartDispatch ?, ?, ?, ?, ?", (ord_nr, line["PartCode"], line["certificate"], line["lotNr"], line["Qty"]))
        cnxn.commit()
    cursor.close()
    cnxn.close()

def create_bom_del_lines(del_lines, ord_nr):
    cnxn = get_db_connection()
    cursor = cnxn.cursor()
    for line in del_lines:
        cursor.execute("SELECT PH.* FROM T_DossierMain DM INNER JOIN T_DossierDetail DL ON DM.DossierCode = DL.DossierCode INNER JOIN T_ProdBOMDeliveryLine BOM ON DL.DossierCode = BOM.DossierCode AND DL.DetailCode = BOM.DetailCode AND DL.DetailSubCode = BOM.DetailSubCode INNER JOIN T_ProdBillOfMat PH ON BOM.ProdBOMLineNr = PH.ProdBOMLineNr AND BOM.ProdHeaderDossierCode = PH.ProdHeaderDossierCode WHERE DM.OrdNr = ? AND DL.PartCode = ? and PH.SubPartCode = ?", (ord_nr, line["ParentPart"], line["PartCode"]))
        exists = cursor.fetchall()
        if not exists:
            cursor.execute("SIP_ins_LEG_BOMDelLine ?, ?, ?, ?", (ord_nr, line["ParentPart"], line["PartCode"], line["Qty"]))
            cnxn.commit()
    cursor.close()
    cnxn.close()

def update_bom_del_lines(del_lines, ord_nr):
    cnxn = get_db_connection()
    cursor = cnxn.cursor()
    for line in del_lines:
        cursor.execute("SIP_upd_LEG_BOMDelLine ?, ?, ?, ?, ?", (ord_nr, line["ParentPart"], line["PartCode"], line["certificate"], line["lotNr"]))
        cnxn.commit()
    cursor.close()
    cnxn.close()

if __name__ == '__main__':                                                                          
    app.run()
    