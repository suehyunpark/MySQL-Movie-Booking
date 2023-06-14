from enum import Enum
from typing import List, Tuple

def format_select_output(headers: List[str], records: List[Tuple], title=None) -> str:
    column_widths = [len(header) for header in headers]
    for record in records:
        for i, value in enumerate(record):
            column_widths[i] = max(column_widths[i], len(str(value)))
    column_widths = [width + 1 for width in column_widths]  # for better readability
    separator_length = sum(column_widths) + len(column_widths) # including gaps between columns
            
    output = '-' * separator_length + '\n'
    if title:
        output += title + '\n'
    output += ' '.join(f"{header:<{width}}" for header, width in zip(headers, column_widths)) + '\n'
    output += '-' * separator_length + '\n'
    for record in records:
        output += ' '.join(f"{field:<{width}}" if field is not None else "None".ljust(width) for field, width in zip(record, column_widths)) + '\n'
    output += '-' * separator_length
    
    return output


class DiscountRate(Enum):
    BASIC = 0.0
    PREMIUM = 0.25
    VIP = 0.5
    
def calculate_reservation_price(orig_price, user_class):
    return round(orig_price * (1 - DiscountRate[user_class.upper()].value), 4)