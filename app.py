from flask import Flask, jsonify, request
from flask_cors import CORS
import datetime
import pyodbc

app = Flask(__name__)
CORS(app)

def get_db_connection():
    server = '192.168.0.121\MSSQLSERVER_ISAH'  # e.g., 'localhost\sqlexpress'
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
    return jsonify(results)

@app.route('/', methods=['POST'])
def handle_post():
    if request.is_json:
        data = request.get_json()
        del_lines = data['delLines']
        del_lines = verify_v1(del_lines)
        old_del_lines = get_del_lines(data['ordNr'])
        
        for line in old_del_lines:
            line['PartCode'] = line['PartCode'].strip()

        import_del_lines = assembly_del_lines_with_scan(del_lines, old_del_lines)

        # del_old_lines(old_del_lines)
        create_new_lines(import_del_lines)

        return ("Scans where imported.")
        
        # return (import_del_lines[0])
    else:
        return jsonify({"error": "Request must be JSON"}), 400


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

def work_data(result):
    worked_data = {
        "ordNr": result[0]["OrdNr"],
        "parts": []
    }

    grouped_by_part_code = {}

    for item in result:
        part_code = item["PartCode"].strip()
        qty = item["Qty"] - item["DelQty"]

        if part_code not in grouped_by_part_code:
            grouped_by_part_code[part_code] = qty
        else:
            grouped_by_part_code[part_code] += qty

    worked_data["parts"] = [{"PartCode": key, "Qty": value} for key, value in grouped_by_part_code.items()]

    return worked_data

def verify_v1(del_lines):
    new_del_lines = []
    for line in del_lines:
        line['PartCode'] = line['partCode']
        line['Qty'] = line['qty']
        if not line['lotNr'].startswith("LF?]"):
            line['certificate'] = ""
            line['certificate'] = line['lotNr']
            line['lotNr'] = ""
        else:
            line['certificate'] = ""
        new_del_lines.append(line)
    return (new_del_lines)

def assembly_del_lines_with_scan(del_lines, old_del_lines):
    # assembly similar del_lines
    sum_dict = {}
    for line in del_lines:
        key = (line['PartCode'], line['lotNr'], line['certificate'])
        sum_dict[key] = sum_dict.get(key, 0) + int(line['Qty'])
    del_lines = [{"PartCode": key[0], "lotNr": key[1], "certificate": key[2], "Qty": qty} for key, qty in sum_dict.items()]
    # set initial import_lines
    import_del_lines = old_del_lines.copy()
    # remove from import_lines lines Parts that where not scanned
    for index, import_line in enumerate(import_del_lines):
        delete = True
        for del_line in del_lines:
            if import_line["PartCode"] == del_line["PartCode"]:
                delete = False
        if delete:
            del import_del_lines[index]
    # produce update import_lines
    for import_line in import_del_lines:
        necessity = int(import_line["Qty"]) - int(import_line["DelQty"])
        while necessity > 0:
            initial_necessity = necessity
            for index, line in enumerate(del_lines):
                if (line["PartCode"] == import_line["PartCode"] and
                    line['certificate'] == import_line["CertificateCode"] and
                    line["lotNr"] == import_line["LotNr"]):
                    if int(line["Qty"]) <= necessity:
                        import_line["ToBeDelQty"] = int(import_line["ToBeDelQty"]) + line["Qty"]
                        necessity = necessity - int(line["Qty"])
                        del del_lines[index]
                        break
                    else:
                        import_line["ToBeDelQty"] = int(import_line["DelQty"]) + necessity
                        line["Qty"] = int(line["Qty"]) - necessity
                        necessity = 0
                        break
            if initial_necessity == necessity:
                break
    # produce insert import_lines
    
    for line in del_lines:
        for import_line in import_del_lines:
            if import_line["PartCode"] == line["PartCode"]:
                line["DossierCode"] = import_line["DossierCode"]
                line["DetailCode"] = import_line["DetailCode"]
                line["DetailSubCode"] = import_line["DetailSubCode"]
                line["DelAddrCode"] = import_line["DelAddrCode"]
                line["ShipAgentCode"] = import_line["ShipAgentCode"]
                line["Remark"] = import_line["Remark"]
                line["WarehouseCode"] = import_line["WarehouseCode"]
                line["LocationCode"] = import_line["LocationCode"]
                line["InventoryStatusCode"] = import_line["InventoryStatusCode"]
                line["UserCode"] = import_line["UserCode"]
                line["PurQty"] = 0
                line["InvtQty"] = 0
                line["ProdQty"] = 0
                line["DelQty"] = 0
                line["ToBeDelQty"] = 0
                line["ToBeDelPurQty"] = 0
                line["ToBeDelInvtQty"] = 0
                line["ToBeDelProdQty"] = 0
                line["DelPurQty"] = 0
                line["DelInvtQty"] = 0
                line["DelProdQty"] = 0
                line["PlanDelDate"] = import_line["PlanDelDate"]
                line["ServObjectCode"] = import_line["ServObjectCode"]
                line["TargetServObjectCode"] = import_line["TargetServObjectCode"]
                line["ReplacedABSLineNr"] = import_line["ReplacedABSLineNr"]
                line["MemoGrpId"] = import_line["MemoGrpId"]
                line["LocationServObjectCode"] = import_line["LocationServObjectCode"]
                line['DelLineLineNr'] = None
                break
        line["ToBeDelQty"] = line["Qty"]
        import_del_lines.append(line)
    return(import_del_lines)

def del_old_lines(old_lines):
    cnxn = get_db_connection()
    cursor = cnxn.cursor()

    for line in old_lines:
        cursor.execute("EXEC IP_del_DeliveryLine ?, ?, ?, ?, ?, ?, ?", (line['DossierCode'], line['DetailCode'], line['DetailSubCode'], line['DelLineLineNr'], 1240000, line['LastUpdatedOn'], 'ISAH'))

    cursor.close()
    cnxn.close()

def create_new_lines(import_lines):
    cnxn = get_db_connection()
    cursor = cnxn.cursor()
    # for line in import_lines:
    #     for value in line.values():
    #         if value == "":
    #             value = "N''"
    for line in import_lines:
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
                    '',
                    '',
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
                    True,
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
            # AUTHORIZE
            cursor.execute("DECLARE @InitLogDate T_DateTime, @ReturnCode tinyint, @DeliveryLinesProcessed int EXEC [IP_prc_DeliveryToShip] ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, @InitLogDate OUTPUT, @ReturnCode OUTPUT, @DeliveryLinesProcessed OUTPUT", (
                line['DossierCode'],
                line['DetailCode'],
                line['DetailSubCode'],
                line['DelLineLineNr'],
                datetime.datetime.now(),
                0,
                None,
                None,
                'ISAH',
                1240000
            ))
            cnxn.commit()
        else:
            # CREATE NEW LINES
            new_DelLineLineNr = None
            params = [
                line.get('DossierCode'),
                line.get('DetailCode'),
                line.get('DetailSubCode'),
                new_DelLineLineNr,
                line.get('DelMainCode'),
                line.get('CustId'),
                line.get('DelAddrCode'),
                line.get('ShipAgentCode'),
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
                datetime.datetime.now(),
                line.get("ConfDelDate"),
                line.get("DelCompletedDate"),
                line.get("DelCompletedInd"),
                line.get("Info"),
                line.get("WarehouseCode"),
                line.get("LocationCode"),
                line.get("lotNr"),
                line.get("certificate"),
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
                True,
                line.get("ServObjectCode"),
                line.get("TargetServObjectCode"),
                line.get("ReplacedABSLineNr"),
                line.get("MultiLevelReplacementInd"),
                1240000,
                line.get("LastUpdatedOn"),
                'ISAH',
                line.get("LocationServObjectCode"),
                line.get("MemoGrpId") 
            ]
            cursor.execute("EXEC IP_ins_DeliveryLine ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?", params)
            cnxn.commit()
            new_DelLineLineNr = params[3]

            # AUTHORIZE
            cursor.execute("DECLARE @InitLogDate T_DateTime, @ReturnCode tinyint, @DeliveryLinesProcessed int EXEC [IP_prc_DeliveryToShip] ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, @InitLogDate OUTPUT, @ReturnCode OUTPUT, @DeliveryLinesProcessed OUTPUT", (
                line['DossierCode'],
                line['DetailCode'],
                line['DetailSubCode'],
                new_DelLineLineNr,
                datetime.datetime.now(),
                0,
                None,
                None,
                'ISAH',
                1240000
            ))
            cnxn.commit()
    cursor.close()
    cnxn.close()

if __name__ == '__main__':
    app.run()
    