class Config:
    AD_SERVER = '192.168.0.2'
    AD_DOMAIN = 'Users'
    AD_BASE_DN = 'OU=Users,OU=Legend Rubber,DC=LEGEND,DC=local'
    AD_USER = 'isahuser'
    AD_PASSWORD = 'GreenBall24#'
    # AD_SERVER = '192.168.67.6'
    # AD_DOMAIN = 'meritdisplay.local'
    # AD_BASE_DN = 'DC=meritdisplay,DC=local'
    # AD_USER = r'meritdisplay\karina.s'
    # AD_PASSWORD = 'RockySnake96'
    

    SERVER = r'LR-SQL01\MSSQLSERVER_ISAH'  # e.g., 'localhost\sqlexpress'
    DATABASE = 'Test_LegendFleet'
    USERNAME = 'IsahIsah'
    PASSWORD = 'isahisah'
    DRIVER = 'SQL Server Native Client 11.0'