from datetime import date


def get_today_date() -> str:
    """Returns today's date as a string in YYYY-MM-DD format."""
    return date.today().strftime("%Y-%m-%d")
