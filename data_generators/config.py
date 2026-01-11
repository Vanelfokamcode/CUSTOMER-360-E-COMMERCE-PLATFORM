"""
Configuration pour la génération de données sales
"""

# Nombre de customers à générer
NUM_CUSTOMERS = 5000

# Pourcentages de problèmes à injecter
DATA_QUALITY_ISSUES = {
    'duplicate_rate': 0.15,        # 15% de duplicates
    'null_email_rate': 0.05,       # 5% d'emails NULL
    'null_phone_rate': 0.10,       # 10% de phones NULL
    'malformed_email_rate': 0.05,  # 5% d'emails malformés
    'mixed_case_rate': 0.30,       # 30% avec casse mixte
    'extra_spaces_rate': 0.20,     # 20% avec espaces inutiles
    'special_chars_rate': 0.08,    # 8% avec caractères bizarres
}

# Formats de dates inconsistants
DATE_FORMATS = [
    '%Y-%m-%d',           # ISO: 2024-01-15
    '%d/%m/%Y',           # European: 15/01/2024
    '%m-%d-%Y',           # American: 01-15-2024
    '%Y/%m/%d %H:%M:%S',  # Timestamp: 2024/01/15 14:30:00
]

# Domaines d'emails (réalistes)
EMAIL_DOMAINS = [
    'gmail.com',
    'yahoo.com', 
    'outlook.com',
    'hotmail.com',
    'protonmail.com',
    'company.com',
]

# Préfixes de téléphone (France, USA, UK)
PHONE_PREFIXES = [
    '+33',   # France
    '+1',    # USA
    '+44',   # UK
]
