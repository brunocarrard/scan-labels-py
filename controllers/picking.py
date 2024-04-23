import datetime
from controllers.db_connection import DatabaseConnection

class Picking:
    def build_picking_list(result):
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
    
    def assembly_del_lines_with_scan_production_bom(del_lines, ord_nr):
        sum_dict = {}
        for line in del_lines:
            key = (line['PartCode'], line['lotNr'], line['certificate'])
            sum_dict[key] = sum_dict.get(key, 0) + int(line['Qty'])
        del_lines = [{"PartCode": key[0], "lotNr": key[1], "certificate": key[2], "Qty": qty} for key, qty in sum_dict.items()]
        cnxn = DatabaseConnection.get_db_connection()
        cursor = cnxn.cursor()
        for line in del_lines:
            cursor.execute("SELECT I.PartCode FROM T_Inventory I INNER JOIN T_CustomFieldValue AS CV ON CV.LookUpValue = I.WarehouseCode INNER JOIN T_ProductionHeader PH ON PH.DossierCode = CV.IsahPrimKey AND CV.FieldDefCode = 'WAREHOUSE' AND CV.IsahTableId = 2 INNER JOIN T_DossierMain DM ON PH.DossierCode = DM.DossierCode WHERE DM.OrdNr = ? AND I.CertificateCode = ? AND I.LotNr = ?", (ord_nr, line["certificate"], line["lotNr"]))
            row = cursor.fetchone()
            if row is None:
                cursor.execute("EXEC SIP_ins_LEG_Inventory ?, ?, ?, ?", (ord_nr, line["PartCode"], line["certificate"], line["lotNr"]))
                cnxn.commit()
            cursor.execute("SIP_ins_LEG_PartDispatch ?, ?, ?, ?, ?", (ord_nr, line["PartCode"], line["certificate"], line["lotNr"], line["Qty"]))
            cnxn.commit()
        cursor.close()
        cnxn.close()