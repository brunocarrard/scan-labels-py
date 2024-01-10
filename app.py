from flask import Flask, jsonify, request
from flask_cors import CORS
import pyodbc

app = Flask(__name__)
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
    print("hello")
    worked_data = {
        "ordNr": result[0]["OrdNr"],
        "parts": []
    }

    grouped_by_part_code = {}

    for item in result:
        part_code = item["PartCode"].strip()
        qty = item["Qty"]

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
        if not line['lotNr'].startswith("LF?]") :
            line['certificate'] = line['lotNr']
            line['lotNr'] = ""
        else:
            line['certificate'] = ""
        new_del_lines.append(line)
    return (new_del_lines)

def assembly_del_lines_with_scan(del_lines, old_del_lines):
    import_del_lines = []
    index_to_be_del = []

    # First part of processing
    for index, old_line in enumerate(old_del_lines.copy()):
        equivalent_indices = [i for i, line in enumerate(del_lines) if line["PartCode"] == old_line["PartCode"]]
        for equivalent_index in equivalent_indices:
            equivalent_line = del_lines[equivalent_index]
            if old_line["Qty"] == int(equivalent_line["Qty"]):
                old_line["Tobedelqty"] = int(equivalent_line["Qty"])
                old_line["certificate"] = equivalent_line["certificate"]
                old_line["lotNr"] = equivalent_line["lotNr"]
                import_del_lines.append(old_line)
                index_to_be_del.append(index)
                del del_lines[equivalent_index]
                break

    old_del_lines = [line for i, line in enumerate(old_del_lines) if i not in index_to_be_del]
    index_to_be_del = []

    # Second part of processing
    for index, old_line in enumerate(old_del_lines):
        total_qty = old_line["Qty"]
        sum_qty = 0
        while sum_qty < total_qty and del_lines:
            del_line_indices = [i for i, line in enumerate(del_lines) if line["PartCode"] == old_line["PartCode"]]
            if del_line_indices:
                del_line_index = del_line_indices[0]
                import_del_line = old_line.copy()
                sum_qty += int(del_lines[del_line_index]["Qty"])
                import_del_line["Qty"] = int(del_lines[del_line_index]["Qty"])
                import_del_line["Tobedelqty"] = int(del_lines[del_line_index]["Qty"])
                import_del_line["certificate"] = del_lines[del_line_index]["certificate"]
                import_del_line["lotNr"] = del_lines[del_line_index]["lotNr"]
                import_del_line["DelLineLineNr"] = None
                del del_lines[del_line_index]
                import_del_lines.append(import_del_line)
            else:
                break
        index_to_be_del.append(index)

    old_del_lines = [line for i, line in enumerate(old_del_lines) if i not in index_to_be_del]

    if any(d.get("DelLineLineNr") is not None for d in import_del_lines):
        for index, line in old_del_lines:
            import_del_lines[index]["DelLineLineNr"] = line["DelLineLineNr"]
            import_del_lines[index]["LastUpdatedOn"] = line["LastUpdatedOn"]

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
            print(line)
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
                    line["Qty"],
                    line["DelPurQty"],
                    line["DelInvtQty"],
                    line["DelProdQty"],
                    line["PlanDelDate"],
                    line["DelDate"],
                    line["ConfDelDate"],
                    line["DelCompletedDate"],
                    line["DelCompletedInd"],
                    line["Info"],
                    line["WarehouseCode"],
                    line["LocationCode"],
                    line["lotNr"],
                    line["certificate"],
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
        else:
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
            if line["DelCompletedDate"] is not None:
                line["DelCompletedDate"] = line["DelCompletedDate"].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            cursor.execute("DECLARE @new_DelLineLineNr T_LineNr EXEC IP_ins_DeliveryLine ?, ?, ?, @new_DelLineLineNr OUTPUT, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?", (
                    line['DossierCode'],
                    line['DetailCode'],
                    line['DetailSubCode'],
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
                    line["Qty"],
                    line["DelPurQty"],
                    line["DelInvtQty"],
                    line["DelProdQty"],
                    line["PlanDelDate"],
                    line["DelDate"],
                    line["ConfDelDate"],
                    line["DelCompletedDate"],
                    line["DelCompletedInd"],
                    line["Info"],
                    line["WarehouseCode"],
                    line["LocationCode"],
                    line["lotNr"],
                    line["certificate"],
                    line["InventoryStatusCode"],
                    line["InvtCreDate"],
                    line["ToBeDelCompletedDosDetInd"],
                    line["ToBeDelCompletedDosDetDate"],
                    line["PlanDelDateDefInd"],
                    line["ConfDelDateDefInd"],
                    line["DelAddrCodeDefInd"],
                    line["CredLimitExceedsInd"],
                    line["CredLimitExceedsDate"],
                    line["AutoCreShipDocInd"],
                    line["CredLimitCheckInd"],
                    line["DelAddrType"],
                    True,
                    line["ServObjectCode"],
                    line["TargetServObjectCode"],
                    line["ReplacedABSLineNr"],
                    line["MultiLevelReplacementInd"],
                    1240000,
                    line["LastUpdatedOn"],
                    'ISAH',
                    line["LocationServObjectCode"],
                    line["MemoGrpId"] 
                ))
            cnxn.commit()
    cursor.close()
    cnxn.close()

if __name__ == '__main__':
    app.run()
    