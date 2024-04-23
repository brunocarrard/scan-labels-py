from controllers.db_connection import DatabaseConnection

class Verification:
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
    
    def verify_lotnr(lot_nrs):
        result = {
            "valid": True,
            "invalid_results": []
        }
        cnxn = DatabaseConnection.get_db_connection()
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
        cnxn = DatabaseConnection.get_db_connection()
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