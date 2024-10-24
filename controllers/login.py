from ldap3 import Server, Connection, SIMPLE, SYNC, ALL, SUBTREE
from config import Config
from controllers.db_connection import DatabaseConnection

class Login:
    def verify_login(username, password):
        server = Server(Config.AD_SERVER, get_info=ALL)
        conn = Connection(server, user=Config.AD_USER, password=Config.AD_PASSWORD, authentication=SIMPLE)
        if not conn.bind():
            return {'authenticated': False, 'message': 'Failed to connect to Active Directory'}
        # Search for the user in Active Directory
        conn.search(Config.AD_BASE_DN, '(sAMAccountName={})'.format(username), SUBTREE,
                attributes=['cn', 'mail'])
        if len(conn.entries) < 1:
            return {'authenticated': False, 'message': 'User not found'}
        # Attempt to authenticate the user with the provided password
        user_dn = conn.entries[0].entry_dn
        user_mail = conn.entries[0].mail
        print(conn.entries)
        conn = Connection(server, user=user_dn, password=password, authentication=SIMPLE)

        if not conn.bind():
            return({'authenticated': False, 'message': 'Invalid credentials'})
        
        return({'authenticated': True, 'message': 'Login successful', 'windows_user': user_mail})
    
    def get_isah_user(windows_user):
        print(str(windows_user))
        response = {
            'found': False,
            'isah_user': '',
            'message': ''
        }
        cnxn = DatabaseConnection.get_db_connection()
        cursor = cnxn.cursor()
        cursor.execute("SELECT UserCode FROM T_UserRegistration WHERE WindowsLogin = ?", (str(windows_user)))
        result = cursor.fetchone()
        if result is not None and len(result) > 0:
            response['isah_user'] = result[0].strip()
            response['found'] = True
        else:
            response['message'] = f'Your email {windows_user} is not linked to an ISAH User'
        cursor.close()
        cnxn.close()
        return response