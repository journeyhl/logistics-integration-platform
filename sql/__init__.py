from pathlib import Path
sql_folder = Path(__file__).parent

def query(name: str, database: str = '') -> str:
    return (sql_folder / 'queries' / database / f'{name}.sql').read_text()

def table(name: str) -> str:
    return (sql_folder / Path('tables') / f'{name}.sql').read_text()

