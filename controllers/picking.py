import datetime
from controllers.db_connection import DatabaseConnection
from collections import Counter, defaultdict
from flask import abort

class Picking:
    def build_picking_list(result):
        worked_data = {
            "ordNr": result[0]["OrdNr"],
            "CustId": result[0]["CustId"],
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
        # repeated parts handle
        ready_repeated_parts_import_del_lines = []
        part_code_counts = Counter(item["PartCode"] for item in import_del_lines if item.get("SubPartInd", 0) != 1)
        repeated_part_codes = {code for code, count in part_code_counts.items() if count > 1}
        repeated_parts_del_lines = [item for item in import_del_lines if item["PartCode"] in repeated_part_codes and item.get("SubPartInd", 0) != 1]
        if len(repeated_parts_del_lines) > 1:
            import_del_lines = [item for item in import_del_lines if item not in repeated_parts_del_lines]
            grouped_repeated_items = defaultdict(list)
            for item in repeated_parts_del_lines:
                grouped_repeated_items[item["PartCode"]].append(item)
            for part_code, items in grouped_repeated_items.items():
                equivalent_line = [line for line in del_lines if line["PartCode"] == part_code]
                totalQty = 0
                if len(equivalent_line) > 1:
                    for line in equivalent_line:
                        totalQty = totalQty + int(line["Qty"])
                else:
                    totalQty = int(equivalent_line[0]["Qty"])
                for index, item in enumerate(items):
                    if totalQty >= int(item["Qty"]):
                        effective_qty = int(item["Qty"])
                        totalQty = totalQty - effective_qty
                    elif totalQty < int(item["Qty"]) and not totalQty == 0:
                        effective_qty = totalQty
                        totalQty = 0
                    elif totalQty == 0:
                        break
                    item["ToBeDelQty"] = int(item["ToBeDelQty"]) + effective_qty
                    item["RemainingQty"] = int(item["Qty"]) - item["ToBeDelQty"]
                    item["ToBeDelCertificateCode"] = equivalent_line[index].get("certificate", "")
                    item["ToBeDelLotNr"] = equivalent_line[index].get("lotNr", "")
                    item["Done"] = True
                    if item["ToBeDelQty"] == item["Qty"]:
                        item["Authorize"] = True
                    else:
                        item["Authorize"] = False
                    equivalent_line[index]["Done"] = True
                    if int(item["InvtQty"]) > 0:
                        item["InvtQty"] = item["Qty"]
                    if int(item["PurQty"]) > 0:
                        item["PurQty"] = item["Qty"]
                    if int(item["ProdQty"]) > 0:
                        item["ProdQty"] = item["Qty"]
                    ready_repeated_parts_import_del_lines.append(item) 
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
        for line in ready_repeated_parts_import_del_lines:
            import_del_lines.append(line)
        return(import_del_lines)
    


    def create_scanned_issue(del_lines, ord_nr, isah_user):
        sum_dict = {}
        for line in del_lines:
            key = (line['PartCode'], line['lotNr'], line['certificate'], line['ParentPart'])
            sum_dict[key] = sum_dict.get(key, 0) + int(line['Qty'])
        del_lines = [{"PartCode": key[0], "lotNr": key[1], "certificate": key[2], "ParentPart": key[3], "Qty": qty} for key, qty in sum_dict.items()]
        cnxn = DatabaseConnection.get_db_connection()
        cursor = cnxn.cursor()
        # verify if there is no stock line beign count.
        for line in del_lines:
            cursor.execute("SELECT I.CycleCountInd FROM T_Inventory I INNER JOIN T_CustomFieldValue AS CV ON CV.LookUpValue = I.WarehouseCode INNER JOIN T_ProductionHeader PH ON PH.DossierCode = CV.IsahPrimKey AND CV.FieldDefCode = 'WAREHOUSE' AND CV.IsahTableId = 2 INNER JOIN T_DossierMain DM ON PH.DossierCode = DM.DossierCode WHERE DM.OrdNr = ? AND I.CertificateCode = ? AND I.LotNr = ?  AND I.PartCode = ?", (ord_nr, line["certificate"], line["lotNr"], line["PartCode"]))
            rows = cursor.fetchall()
            found_one = False
            found_zero = False
            for row in rows:
                if row[0]:
                    found_one = True
                else:
                    found_zero = True
            if found_one and not found_zero:
                abort(400, description=f"Part {line["PartCode"]} is on stock count, try again later.")

        for line in del_lines:
            cursor.execute("""
                        SELECT  CASE
                                    WHEN BOM.InvtQty > 0 THEN 1
                                    WHEN BOM.ProdQty > 0 THEN 3
                                    WHEN BOM.PurQty > 0 THEN 1
                                END 'type'
                        FROM T_DossierMain DM
                        INNER JOIN T_DossierDetail DD ON DD.DossierCode = DM.DossierCode
                        INNER JOIN ProdHeadDosDetLink PHDL ON DD.DossierCode = PHDL.DossierCode AND DD.DetailCode = PHDL.DetailCode AND DD.DetailSubCode = PHDL.DetailSubCode
                        INNER JOIN T_ProductionHeader PH ON PH.ProdHeaderDossierCode = PHDL.ProdHeaderDossierCode
                        INNER JOIN T_ProdBillOfMat BOM ON PHDL.ProdHeaderDossierCode = BOM.ProdHeaderDossierCode
                        WHERE DM.OrdNr = ? AND DD.PartCode = ? AND BOM.SubPartCode = ?
                           """, (ord_nr, line["ParentPart"], line["PartCode"]))
            row = cursor.fetchone()
            type = row[0]
            if type == 1:
                cursor.execute("SELECT I.CycleCountInd FROM T_Inventory I INNER JOIN T_CustomFieldValue AS CV ON CV.LookUpValue = I.WarehouseCode INNER JOIN T_ProductionHeader PH ON PH.DossierCode = CV.IsahPrimKey AND CV.FieldDefCode = 'WAREHOUSE' AND CV.IsahTableId = 2 INNER JOIN T_DossierMain DM ON PH.DossierCode = DM.DossierCode WHERE DM.OrdNr = ? AND I.CertificateCode = ? AND I.LotNr = ?  AND I.PartCode = ?", (ord_nr, line["certificate"], line["lotNr"], line["PartCode"]))
                rows = cursor.fetchall()
                if not rows:
                    cursor.execute("EXEC SIP_ins_LEG_Inventory ?, ?, ?, ?", (ord_nr, line["PartCode"], line["certificate"], line["lotNr"]))
                    cnxn.commit()
            cursor.execute("SIP_ins_LEG_PartDispatch ?, ?, ?, ?, ?, ?, ?", (ord_nr, line["PartCode"], line["ParentPart"], line["certificate"], line["lotNr"], line["Qty"], isah_user))
            cnxn.commit()
        cursor.close()
        cnxn.close()

    def VerifyAMZUSOrder(picking_list):
        cnxn = DatabaseConnection.get_db_connection()
        cursor = cnxn.cursor()
        for part in picking_list["parts"]:
            if part.get("SubPartInd") is not None:
                cursor.execute("""
                    SELECT SUM(PH.ReceiptQty) 'ReceiptQty'
                    FROM DossierMain DM
                    INNER JOIN DossierDetail DD ON DM.DossierCode = DD.DossierCode
                    INNER JOIN ProdHeadDosDetLink PHDL ON PHDL.DossierCode = DD.DossierCode AND PHDL.DetailCode = DD.DetailCode AND PHDL.DetailSubCode = DD.DetailSubCode
                    INNER JOIN LogProdReceiptProdHeader PH ON PHDL.ProdHeaderDossierCode = PH.ProdHeaderDossierCode
                    WHERE DM.OrdNr = ? AND DD.PartCode = ?
                """, (picking_list["ordNr"], part["PartCode"]))
                result = cursor.fetchone()
                receipt_qty_sum = result[0] if result[0] is not None else 0
                if not receipt_qty_sum == part["Qty"]:
                    return False
            # else:
            #     cursor.execute("""
            #         SELECT SUM(PH.ReceiptQty) 'ReceiptQty'
            #         FROM DossierMain DM
            #         INNER JOIN DossierDetail DD ON DM.DossierCode = DD.DossierCode
            #         INNER JOIN ProdHeadDosDetLink PHDL ON PHDL.DossierCode = DD.DossierCode AND PHDL.DetailCode = DD.DetailCode AND PHDL.DetailSubCode = DD.DetailSubCode
            #         INNER JOIN LogProdReceiptProdHeader PH ON PHDL.ProdHeaderDossierCode = PH.ProdHeaderDossierCode
            #         WHERE DM.OrdNr = ? AND DD.PartCode = ?
            #     """, (picking_list["ordNr"], part["PartCode"]))
            #     result = cursor.fetchone()
            #     receipt_qty_sum = result[0] if result[0] is not None else 0
            #     if not receipt_qty_sum == part["Qty"]:
            #         return False
        cursor.close()
        cnxn.close()