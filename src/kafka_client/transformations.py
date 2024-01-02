import re
from unidecode import unidecode
from src.constants import COLUMNS_TO_NORMALIZE, COLUMNS_TO_KEEP


def merge_two_columns(
    col_a: str, col_b: str, row: dict, normalize: bool = True
) -> dict:
    val_col_a = row.get(col_a)
    val_col_b = row.get(col_b)
    new_col = ""
    next_start_char = ""
    if val_col_a:
        new_col = val_col_a
        next_start_char = "\n"
    if val_col_b:
        new_col += next_start_char + val_col_b
    final = new_col or None
    if final and normalize:
        final = normalize_one(final)
    return final


def separate_commercialisation_dates(row: dict) -> tuple:
    date_debut_fin = row.get("date_debut_fin_de_commercialisation")
    if not date_debut_fin:
        return None, None

    date_debut_commercialisation = None
    date_fin_commercialisation = None
    date_pattern = r"(\d{2}/\d{2}/\d{4})"
    patterns = re.findall(date_pattern, date_debut_fin)
    if len(patterns) == 2:
        date_debut_commercialisation = patterns[0]
        date_fin_commercialisation = patterns[1]
    elif len(patterns) == 1:
        if "depuis le" in date_debut_fin.lower():
            date_debut_commercialisation = patterns[0]
        elif "jusqu" in date_debut_fin.lower():
            date_fin_commercialisation = patterns[0]
    return date_debut_commercialisation, date_fin_commercialisation


def normalize_one(text: str) -> str:
    """
    Remove accents.
    """
    return unidecode(text)


def normalize_columns(api_row: dict) -> dict:
    kafka_row = {}
    for col in COLUMNS_TO_KEEP:
        kafka_row[col] = api_row.get(col)
    for col in COLUMNS_TO_NORMALIZE:
        if not api_row.get(col):
            kafka_row[col] = None
            continue
        kafka_row[col] = normalize_one(api_row[col])

    return kafka_row


def transform_row(api_row: dict) -> dict:
    kafka_row = normalize_columns(api_row)

    kafka_row["risques_pour_le_consommateur"] = merge_two_columns(
        "risques_encourus_par_le_consommateur",
        "description_complementaire_du_risque",
        api_row,
    )
    kafka_row["recommandations_sante"] = merge_two_columns(
        "preconisations_sanitaires",
        "conduites_a_tenir_par_le_consommateur",
        api_row,
    )
    kafka_row["informations_complementaires"] = merge_two_columns(
        "informations_complementaires",
        "informations_complementaires_publiques",
        api_row,
    )
    sep_columns = separate_commercialisation_dates(api_row)
    kafka_row["date_debut_commercialisation"] = sep_columns[0]
    kafka_row["date_fin_commercialisation"] = sep_columns[1]
    return kafka_row
