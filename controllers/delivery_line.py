from controllers.db_connection import DatabaseConnection
import datetime

class DeliveryLines:
    def create_new_lines(import_lines):
        cnxn = DatabaseConnection.get_db_connection()
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
                        line["WarehouseCode"],
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
        DeliveryLines.update_created_lines(import_lines)

    def update_created_lines(import_lines):
        cnxn = DatabaseConnection.get_db_connection()
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
        cnxn = DatabaseConnection.get_db_connection()
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

    # def create_bom_del_lines(del_lines, ord_nr):
    #     cnxn = Connection.get_db_connection()
    #     cursor = cnxn.cursor()
    #     for line in del_lines:
    #         cursor.execute("SELECT PH.* FROM T_DossierMain DM INNER JOIN T_DossierDetail DL ON DM.DossierCode = DL.DossierCode INNER JOIN T_ProdBOMDeliveryLine BOM ON DL.DossierCode = BOM.DossierCode AND DL.DetailCode = BOM.DetailCode AND DL.DetailSubCode = BOM.DetailSubCode INNER JOIN T_ProdBillOfMat PH ON BOM.ProdBOMLineNr = PH.ProdBOMLineNr AND BOM.ProdHeaderDossierCode = PH.ProdHeaderDossierCode WHERE DM.OrdNr = ? AND DL.PartCode = ? and PH.SubPartCode = ?", (ord_nr, line["ParentPart"], line["PartCode"]))
    #         exists = cursor.fetchall()
    #         if not exists:
    #             cursor.execute("SIP_ins_LEG_BOMDelLine ?, ?, ?, ?", (ord_nr, line["ParentPart"], line["PartCode"], line["Qty"]))
    #             cnxn.commit()
    #     cursor.close()
    #     cnxn.close()

    # def update_bom_del_lines(del_lines, ord_nr):
    #     cnxn = Connection.get_db_connection()
    #     cursor = cnxn.cursor()
    #     for line in del_lines:
    #         cursor.execute("SIP_upd_LEG_BOMDelLine ?, ?, ?, ?, ?", (ord_nr, line["ParentPart"], line["PartCode"], line["certificate"], line["lotNr"]))
    #         cnxn.commit()
    #     cursor.close()
    #     cnxn.close()