from controllers.db_connection import DatabaseConnection
import pyodbc

class Getters:
    def get_sub_parts(ord_nr):
        try:
            cnxn = DatabaseConnection.get_db_connection()
            cursor = cnxn.cursor()
            
            # Execute the stored procedure with a parameter
            cursor.execute("EXEC SIP_sel_LEG_StockMovementsBOM ?", ord_nr)
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            # Get column names from the cursor description
            columns = [column[0] for column in cursor.description]
            
            # Process each row into a dictionary
            results = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                # Strip whitespace from string values
                for key, value in row_dict.items():
                    if isinstance(value, str):
                        row_dict[key] = value.strip()
                results.append(row_dict)
            
            # Return results as JSON
            return results
    
        except pyodbc.Error as e:
            # Handle any database errors
            return
        
        finally:
            # Ensure the cursor and connection are closed
            cursor.close()
            cnxn.close()
    
    def get_del_lines(ord_nr):
        cnxn = DatabaseConnection.get_db_connection()
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
        cnxn = DatabaseConnection.get_db_connection()
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
        cnxn = DatabaseConnection.get_db_connection()
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
    
    def get_warehouses():
        cnxn = DatabaseConnection.get_db_connection()
        cursor = cnxn.cursor()
        cursor.execute("SELECT DISTINCT WarehouseCode FROM T_Warehouse WHERE WarehouseCode <> N''")
        rows = cursor.fetchall()

        columns = [column[0] for column in cursor.description]
        results = []
        for row in rows:
            row_dict = dict(zip(columns, row))

            # Strip whitespace from string values and format date values
            for key, value in row_dict.items():
                results.append(value.strip())

        cursor.close()
        cnxn.close()
        return results
    
    def get_inventory_parts(warehouse):
        cnxn = DatabaseConnection.get_db_connection()
        cursor = cnxn.cursor()
        cursor.execute("SELECT DISTINCT PartCode FROM T_Inventory WHERE WarehouseCode = ?", (warehouse))
        rows = cursor.fetchall()

        columns = [column[0] for column in cursor.description]
        results = []
        for row in rows:
            row_dict = dict(zip(columns, row))

            # Strip whitespace from string values and format date values
            for key, value in row_dict.items():
                results.append(value.strip())

        cursor.close()
        cnxn.close()
        return results