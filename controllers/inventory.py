from controllers.db_connection import DatabaseConnection

class Inventory:
    def stock_movement(payload):
        cnxn = DatabaseConnection.get_db_connection()
        cursor = cnxn.cursor()
        cursor.execute("EXEC IP_prc_LocationWIPInvtMove ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?", (payload))
        cnxn.commit()
        cursor.close()
        cnxn.close()