"""
Génère des données customers intentionnellement sales
pour simuler des problèmes réels de data quality
"""

import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
from config import *

# Seed pour reproducibilité
Faker.seed(42)
random.seed(42)
fake = Faker(['fr_FR', 'en_US', 'en_GB'])


def generate_clean_customer():
    """Génère un customer 'propre' (avant de le casser)"""
    return {
        'customer_id': fake.uuid4(),
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'email': fake.email(),
        'phone': fake.phone_number(),
        'address': fake.street_address(),
        'city': fake.city(),
        'country': fake.country_code(),
        'created_at': fake.date_time_between(start_date='-2y', end_date='now'),
    }


def inject_data_quality_issues(customer):
    """
    Injecte des problèmes de qualité dans un customer
    """
    
    # PROBLÈME 1: Email NULL (5% du temps)
    if random.random() < DATA_QUALITY_ISSUES['null_email_rate']:
        customer['email'] = None
    
    # PROBLÈME 2: Email malformé (5% du temps) - FIX: vérifier si email existe
    elif customer['email'] and random.random() < DATA_QUALITY_ISSUES['malformed_email_rate']:
        email = customer['email']
        issues = [
            email.replace('@', ''),
            email.replace('.', ''),
            email.replace('@', '@@'),
            email.replace('.com', 'com'),
            'invalid_' + email,
        ]
        customer['email'] = random.choice(issues)
    
    # PROBLÈME 3: Casse mixte (30% du temps)
    if random.random() < DATA_QUALITY_ISSUES['mixed_case_rate']:
        customer['first_name'] = random.choice([
            customer['first_name'].upper(),
            customer['first_name'].lower(),
            customer['first_name'].title(),
        ])
    
    # PROBLÈME 4: Espaces inutiles (20% du temps)
    if random.random() < DATA_QUALITY_ISSUES['extra_spaces_rate']:
        customer['first_name'] = '  ' + customer['first_name'] + '  '
        customer['last_name'] = '  ' + customer['last_name'] + ' '
        if customer['email']:
            customer['email'] = ' ' + customer['email'] + ' '
    
    # PROBLÈME 5: Phone NULL (10% du temps)
    if random.random() < DATA_QUALITY_ISSUES['null_phone_rate']:
        customer['phone'] = None
    
    # PROBLÈME 6: Caractères spéciaux dans nom (8% du temps)
    if random.random() < DATA_QUALITY_ISSUES['special_chars_rate']:
        customer['first_name'] = customer['first_name'] + '™'
        customer['last_name'] = '©' + customer['last_name']
    
    # PROBLÈME 7: Format de date inconsistant
    if not isinstance(customer['created_at'], str):
        date_format = random.choice(DATE_FORMATS)
        customer['created_at'] = customer['created_at'].strftime(date_format)
    
    return customer


def create_duplicate(original_customer):
    """Crée un duplicate d'un customer existant"""
    duplicate = original_customer.copy()
    
    # Variations typiques de duplicates
    variations = [
        lambda c: {**c, 'first_name': c['first_name'] + 'x'},
        lambda c: {**c, 'email': 'new_' + c['email'] if c['email'] else None},
        lambda c: {**c, 'first_name': c['first_name'].upper()},
        lambda c: {**c, 'phone': fake.phone_number()},
    ]
    
    duplicate = random.choice(variations)(duplicate)
    duplicate['customer_id'] = fake.uuid4()
    
    return duplicate


def generate_messy_dataset(num_customers=NUM_CUSTOMERS):
    """Génère le dataset complet avec tous les problèmes"""
    customers = []
    
    num_unique = int(num_customers * (1 - DATA_QUALITY_ISSUES['duplicate_rate']))
    num_duplicates = num_customers - num_unique
    
    print(f"Génération de {num_unique} customers uniques...")
    
    for i in range(num_unique):
        customer = generate_clean_customer()
        customer = inject_data_quality_issues(customer)
        customers.append(customer)
        
        if (i + 1) % 1000 == 0:
            print(f"  → {i + 1}/{num_unique} générés")
    
    print(f"\nGénération de {num_duplicates} duplicates...")
    
    for i in range(num_duplicates):
        original = random.choice(customers)
        duplicate = create_duplicate(original)
        duplicate = inject_data_quality_issues(duplicate)
        customers.append(duplicate)
        
        if (i + 1) % 100 == 0:
            print(f"  → {i + 1}/{num_duplicates} duplicates créés")
    
    random.shuffle(customers)
    
    return pd.DataFrame(customers)


def analyze_data_quality(df):
    """Analyse et affiche les problèmes de qualité"""
    total = len(df)
    
    print("\n" + "="*60)
    print("📊 DATA QUALITY REPORT")
    print("="*60)
    
    print(f"\nTotal customers: {total:,}")
    
    null_emails = df['email'].isna().sum()
    null_phones = df['phone'].isna().sum()
    print(f"\n🔴 NULL values:")
    print(f"  - Emails NULL: {null_emails} ({null_emails/total*100:.1f}%)")
    print(f"  - Phones NULL: {null_phones} ({null_phones/total*100:.1f}%)")
    
    valid_emails = df['email'].dropna().str.contains('@').sum()
    invalid_emails = len(df['email'].dropna()) - valid_emails
    print(f"\n🔴 Malformed emails: {invalid_emails} ({invalid_emails/total*100:.1f}%)")
    
    spaces_in_names = df['first_name'].str.contains('  ').sum()
    print(f"\n🔴 Extra spaces: {spaces_in_names} ({spaces_in_names/total*100:.1f}%)")
    
    duplicate_emails = df[df['email'].notna()].duplicated(subset=['email']).sum()
    print(f"\n🔴 Potential duplicates (same email): {duplicate_emails} ({duplicate_emails/total*100:.1f}%)")
    
    print(f"\n📅 Date formats detected:")
    date_samples = df['created_at'].sample(min(5, len(df))).tolist()
    for i, sample in enumerate(date_samples, 1):
        print(f"  {i}. {sample}")
    
    print("\n" + "="*60)


if __name__ == "__main__":
    print("🔧 Génération de données customers (intentionnellement sales)...\n")
    
    df = generate_messy_dataset(NUM_CUSTOMERS)
    analyze_data_quality(df)
    
    output_path = '../data/messy_customers.csv'
    df.to_csv(output_path, index=False)
    
    print(f"\n✅ Dataset sauvegardé: {output_path}")
    print(f"📦 Taille: {len(df):,} lignes x {len(df.columns)} colonnes")
    print(f"💾 Fichier: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    print("\n🎯 PROCHAINE ÉTAPE:")
    print("   Charger ce CSV sale dans PostgreSQL (raw.csv_customers)")
