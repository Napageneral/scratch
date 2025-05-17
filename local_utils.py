import re
from functools import lru_cache

# Pre-compile regex for better performance
RE_NON_DIGIT = re.compile(r'\D')

@lru_cache(maxsize=10000)
def normalize_phone_number(phone_number: str) -> str:
    """
    Normalize a phone number by removing non-digits and potentially removing
    leading country code (1 for US numbers).
    
    This function is cached using lru_cache to avoid redundant processing
    for repeatedly seen phone numbers.
    """
    if not phone_number:
        return ""
    # Basic normalization: remove non-digits using pre-compiled regex
    digits = RE_NON_DIGIT.sub('', phone_number)
    # Attempt to remove leading country code like '1' for US numbers if it's 11 digits
    if len(digits) == 11 and digits.startswith('1'):
        return digits[1:]
    return digits

def clean_contact_name(name: str) -> str:
    if not name:
        return ""
    return name.strip()

def _safe_timestamp(ts):
    # This function was imported in etl_chats.py but might be unused or
    # its functionality covered by other timestamp conversions.
    # Providing a pass-through implementation for now.
    return ts 