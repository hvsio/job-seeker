from datetime import datetime
import re
import numbers


def extract_numerical_salary(salary_entry: numbers.Number, exchange_rate=1) -> numbers.Number:
    """Retrieve only numerical value of salary. Crop any literals and currency special symbols. Choose the minimum salary if range is provided.

    Args:
        salary_entry (float): job postings salary

    Returns:
        float | None : numerical salary value or None
    """
    if isinstance(salary_entry, numbers.Number): return salary_entry
    elif salary_entry and salary_entry != '':
        print(salary_entry, type(salary_entry))
        found_numericals = list(filter(lambda x: len(x) != 0, re.findall(r'\d{0,6}', salary_entry)))
        found_numericals = [float(numerical) for numerical in found_numericals]
        min_salary = min(found_numericals)
        return min_salary * exchange_rate
    else: return None


def extract_location(location: str) -> str:
    """Extract enumerated location to retrieve only the city.

    Args:
        location (str): location expression

    Returns:
        str: extracted town
    """
    return location.split(',')[0].replace("'", '')


def unify_date_format(date: datetime) -> datetime:
    """Apply a common date format to the DB entries

    Args:
        date (datetime): returned date in various fromat

    Returns:
        datetime: date formatted as dd/mm/YYYY
    """
    return datetime.fromisoformat(str(date)).strftime(
        '%d/%m/%Y'
    )
    
