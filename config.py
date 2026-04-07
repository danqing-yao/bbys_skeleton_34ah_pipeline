CONFIG = {
    "bq_project": "barnebys-skeleton",
    "bq_dataset": "42ah",
    "skeleton_start": "2025-01-01",
    "skeleton_end":   "2026-01-01",
    "bbys_aws_start": "2024-11-01",
    "bbys_aws_end":   "2025-05-31",
    "bbys_azure_start": "2025-06-01",
    "bbys_azure_end":   "2025-12-31",
}

SKELETON_HOUSES = [
    {"ah_id": "3915", "database": "Arosfrimarken"},
    {"ah_id": "3952", "database": "Auksjonshallen"},
    {"ah_id": "3862", "database": "Bastionen"},
    {"ah_id": "3900", "database": "CarlssonRing"},
    {"ah_id": "3902", "database": "Conap"},
    {"ah_id": "3898", "database": "Dahlstroms"},
    {"ah_id": "44",   "database": "Dalarnas"},
    {"ah_id": "53",   "database": "Dyrgripen"},
    {"ah_id": "230",  "database": "EekAuksjon"},
    {"ah_id": "3922", "database": "GunGarage"},
    {"ah_id": "90",   "database": "Ingelmark"},
    {"ah_id": "3768", "database": "Jamtloppan"},
    {"ah_id": "3031", "database": "kanonauktioner"},
    {"ah_id": "3601", "database": "Karljohan"},
    {"ah_id": "3756", "database": "Karlssons"},
    {"ah_id": "2949", "database": "KnutsonBloom"},
    {"ah_id": "3849", "database": "LaholmsHAP"},
    {"ah_id": "107",  "database": "Lpfoto"},
    {"ah_id": "2659", "database": "MyntAuktioner"},
    {"ah_id": "3925", "database": "Malardalen"},
    {"ah_id": "72",   "database": "NyaHallands"},
    {"ah_id": "3687", "database": "Olsens"},
    {"ah_id": "125",  "database": "Probus"},
    {"ah_id": "3865", "database": "Rekomo"},
    {"ah_id": "3950", "database": "McDonald"},
    {"ah_id": "3723", "database": "Snapphane"},
    {"ah_id": "3923", "database": "StoreMaele"},
    {"ah_id": "3904", "database": "Sydklippet"},
    {"ah_id": "283",  "database": "Sodersen"},
    {"ah_id": "3764", "database": "Solvesen"},
    {"ah_id": "3722", "database": "Upplands"},
    {"ah_id": "3869", "database": "Wunderkammer"},
    {"ah_id": "3663", "database": "Ystads"},
    {"ah_id": "3916", "database": "Ostlandet"}

]

SKELETON_CONN_BASE = {
    "server": "bby-eu1-sql-prod.database.windows.net",
}

BBYS_AWS_CONN = {
    "ssh_host":       "34.241.11.150",
    "ssh_port":       4862,
    "ssh_username":   "ubuntu",
    "ssh_key_file":   "/Users/danqing/Downloads/bbys_tech_eu.pem",
    "mysql_host":     "172.31.40.226",
    "mysql_port":     3306,
    "mysql_user":     "root",
    "mysql_password": "j5C!9X1PnO27",
    "mysql_db":       "barnebys"
}

BBYS_AZURE_CONN = {
    "host":     "barnebys.mysql.database.azure.com",
    "database": "barnebys",
    "username": "readonly_user",
    "password": "fp3pjeM^Ed^YfbK^yqPz*$LTlR"
}

AH_IDS = ['3915', '3952', '3862', '3900', '3902', '3898', '44',
           '53', '230', '3922', '90', '3768', '3031', '3601', '3756', '2949', 
           '3849', '107', '2659', '3925', '72', '3687', '125', '3865', '3950', '3723', 
           '3923', '3904', '283', '3764', '3722', '3869', '3663', '3916']